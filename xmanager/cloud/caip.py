# Copyright 2021 DeepMind Technologies Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Client for interacting with AI Platform (Unified).

https://cloud.google.com/ai-platform-unified/docs/reference/rest
"""
import asyncio
import functools
import logging
import math
import time
from typing import Any, Dict, List, Optional, Sequence, Union

import attr
from google.cloud import aiplatform
from google.cloud import aiplatform_v1 as aip_v1
from google.cloud import aiplatform_v1beta1 as aip_v1beta
import immutabledict

from xmanager import xm
from xmanager.cloud import auth
from xmanager.xm import resources as xm_resources
from xmanager.xm import utils
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import execution as local_execution
from xmanager.xm_local import executors as local_executors

_DEFAULT_LOCATION = 'us-central1'
# The only machines available on AI Platform are N1 machines.
# https://cloud.google.com/ai-platform-unified/docs/predictions/machine-types#machine_type_comparison
# TODO: Add machines that support A100.
# TODO: Move lookup to a canonical place.
_MACHINE_TYPE_TO_CPU_RAM = {
    'n1-standard-4': (4, 15),
    'n1-standard-8': (8, 30),
    'n1-standard-16': (16, 60),
    'n1-standard-32': (32, 120),
    'n1-standard-64': (64, 240),
    'n1-standard-96': (96, 360),
    'n1-highmem-2': (2, 13),
    'n1-highmem-4': (4, 26),
    'n1-highmem-8': (8, 52),
    'n1-highmem-16': (16, 104),
    'n1-highmem-32': (32, 208),
    'n1-highmem-64': (64, 416),
    'n1-highmem-96': (96, 624),
    'n1-highcpu-16': (16, 14),
    'n1-highcpu-32': (32, 28),
    'n1-highcpu-64': (64, 57),
    'n1-highcpu-96': (96, 86),
}

# Hide noisy warning regarding:
# `file_cache is unavailable when using oauth2client >= 4.0.0 or google-auth`
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)


@functools.lru_cache()
def client():
  # Global singleton defers client creation until an actual launch.
  # If the user only launches local jobs, they don't need cloud access.
  return Client()


def _caip_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, local_executors.Caip)


class Client:
  """Client class for interacting with AI Platform (Unified)."""

  def __init__(self,
               project: Optional[str] = None,
               location: str = _DEFAULT_LOCATION) -> None:
    """Create a client.

    Args:
      project: GCP project ID.
      location: Location of the API endpoint. Defaults to region us-central1.
        https://cloud.google.com/vertex-ai/docs/reference/rest#service-endpoint
    """
    self.location = location
    self.project = project or auth.get_project_name()
    self.parent = f'projects/{self.project}/locations/{self.location}'
    # TODO: move staging_bucket when issue is fixed
    # https://github.com/googleapis/python-aiplatform/pull/421
    aiplatform.init(
        project=self.project,
        location=self.location,
        staging_bucket=f'gs://{auth.get_bucket()}')

  def launch(self, work_unit_name: str, jobs: Sequence[xm.Job]) -> str:
    """Launch jobs on AI Platform (Unified)."""
    pools = []
    for job in jobs:
      executable = job.executable
      if not isinstance(executable,
                        local_executables.GoogleContainerRegistryImage):
        raise ValueError('Executable {} has type {}. Executable must be of '
                         'type GoogleContainerRegistryImage'.format(
                             executable, type(executable)))

      args = utils.to_command_line_args(
          xm.merge_args(executable.args, job.args))
      env_vars = {**executable.env_vars, **job.env_vars}
      env = [{'name': k, 'value': v} for k, v in env_vars.items()]
      if job.executor.resources.is_tpu_job:
        tpu_runtime_version = 'nightly'  # pylint: disable=unused-variable
        if job.executor.tpu_capability:
          tpu_runtime_version = job.executor.tpu_capability.tpu_runtime_version
      pools.append(
          aip_v1.WorkerPoolSpec(
              machine_spec=get_machine_spec(job),
              container_spec=aip_v1.ContainerSpec(
                  image_uri=executable.image_path,
                  args=args,
                  env=env,
                  # TODO: The `tpuTfVersion` field doesn't exist
                  # yet in AI Platform (Unified). This is a placeholder
                  # based on the legacy AI Platform ReplicaConfig.
                  # https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs#ReplicaConfig
                  # 'tpuTfVersion': tpu_runtime_version,
              ),
              replica_count=1,
          ))

    # TOOD(chenandrew): CAIP only allows for 4 worker pools.
    # If CAIP does not implement more worker pools, another work-around
    # is to simply put all jobs into the same worker pool as replicas.
    # Each replica would contain the same image, but would execute different
    # modules based the replica index. A disadvantage of this implementation
    # is that every replica must have the same machine_spec.
    if len(pools) > 4:
      raise ValueError('Cloud Job for xm jobs {} contains {} worker types. '
                       'Only 4 worker types are supported'.format(
                           jobs, len(pools)))

    custom_job = aiplatform.CustomJob(
        project=self.project,
        location=self.location,
        display_name=work_unit_name,
        worker_pool_specs=pools,
    )
    custom_job.run(sync=False)

    time.sleep(5)  # Wait for custom_job.run to execute asynchronously.
    print(f'Job launched at: {custom_job._dashboard_uri()}')  # pylint: disable=protected-access
    return custom_job.resource_name

  async def wait_for_job(self, job_name: str) -> None:
    job = aiplatform.CustomJob.get(job_name)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, job._block_until_complete)  # pylint: disable=protected-access

  async def create_tensorboard(self, name: str) -> str:
    """Create a CAIP tensorboard instance."""
    tensorboard_client = aip_v1beta.TensorboardServiceAsyncClient(
        client_options={
            'api_endpoint': f'{self.location}-aiplatform.googleapis.com'
        })
    tensorboard = aip_v1beta.Tensorboard(display_name=name)
    op = await tensorboard_client.create_tensorboard(
        aip_v1beta.CreateTensorboardRequest(
            parent=self.parent,
            tensorboard=tensorboard,
        ))
    return (await op.result()).name


_CLOUD_TPU_ACCELERATOR_TYPES = immutabledict.immutabledict({
    xm.ResourceType.V2: 'TPU_V2',
    xm.ResourceType.V3: 'TPU_V3',
})


def get_machine_spec(job: xm.Job) -> Dict[str, Any]:
  """Get the GCP machine type that best matches the Job's resources."""
  assert isinstance(job.executor, local_executors.Caip)
  resources = job.executor.resources
  machine_type = cpu_ram_to_machine_type(
      resources.task_requirements.get(xm.ResourceType.CPU),
      resources.task_requirements.get(xm.ResourceType.RAM))
  spec = {'machine_type': machine_type}
  for resource, value in resources.task_requirements.items():
    accelerator_type = None
    if xm_resources.is_gpu(resource):
      accelerator_type = 'NVIDIA_TESLA_' + str(resource).upper()
    elif xm_resources.is_tpu(resource):
      accelerator_type = _CLOUD_TPU_ACCELERATOR_TYPES[resource]
    if accelerator_type:
      spec['accelerator_type'] = aip_v1.AcceleratorType[accelerator_type]
      spec['accelerator_count'] = value
  return spec


@attr.s(auto_attribs=True)
class CaipHandle(local_execution.ExecutionHandle):
  """A handle for referring to the launched container."""

  job_name: str

  async def wait(self) -> None:
    await client().wait_for_job(self.job_name)


# Must act on all jobs with `local_executors.Caip` executor.
def launch(work_unit_name: str, job_group: xm.JobGroup) -> List[CaipHandle]:
  """Launch CAIP jobs in the job_group and return a handler."""
  jobs = utils.collect_jobs_by_filter(job_group, _caip_job_predicate)
  # As client creation may throw, do not initiate it if there are no jobs.
  if not jobs:
    return []

  job_name = client().launch(
      work_unit_name=work_unit_name,
      jobs=jobs,
  )
  return [CaipHandle(job_name=job_name)]


def cpu_ram_to_machine_type(cpu: Optional[int],
                            ram: Optional[Union[int, float]]) -> str:
  """Convert a cpu and memory spec into a machine type."""
  if cpu is None or ram is None:
    return 'n1-standard-4'

  optimal_machine_type = ''
  optimal_excess_resources = math.inf
  for machine_type, (machine_cpu,
                     machine_ram) in _MACHINE_TYPE_TO_CPU_RAM.items():
    if machine_cpu >= cpu and machine_ram >= ram:
      excess = machine_cpu + machine_ram - cpu - ram
      if excess < optimal_excess_resources:
        optimal_machine_type = machine_type
        optimal_excess_resources = excess

  if optimal_machine_type:
    return optimal_machine_type
  raise ValueError(
      '(cpu={}, ram={}) does not fit in any valid machine type'.format(
          cpu, ram))
