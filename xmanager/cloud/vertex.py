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
"""Client for interacting with Vertex AI.

https://cloud.google.com/vertex-ai/docs/reference/rest
"""
import asyncio
import logging
import math
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

import attr
from google.cloud import aiplatform
from google.cloud import aiplatform_v1 as aip_v1
from xmanager import xm
from xmanager.cloud import auth
from xmanager.xm import utils
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import execution as local_execution
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local import status as local_status

_DEFAULT_LOCATION = 'us-central1'
# The only machines available on AI Platform are N1 machines and A2 machines.
# https://cloud.google.com/ai-platform-unified/docs/predictions/machine-types#machine_type_comparison
# TODO: Move lookup to a canonical place.
_MACHINE_TYPE_TO_CPU_RAM = {
    'n1-standard-4': (4, 15 * xm.GiB),
    'n1-standard-8': (8, 30 * xm.GiB),
    'n1-standard-16': (16, 60 * xm.GiB),
    'n1-standard-32': (32, 120 * xm.GiB),
    'n1-standard-64': (64, 240 * xm.GiB),
    'n1-standard-96': (96, 360 * xm.GiB),
    'n1-highmem-2': (2, 13 * xm.GiB),
    'n1-highmem-4': (4, 26 * xm.GiB),
    'n1-highmem-8': (8, 52 * xm.GiB),
    'n1-highmem-16': (16, 104 * xm.GiB),
    'n1-highmem-32': (32, 208 * xm.GiB),
    'n1-highmem-64': (64, 416 * xm.GiB),
    'n1-highmem-96': (96, 624 * xm.GiB),
    'n1-highcpu-16': (16, 14 * xm.GiB),
    'n1-highcpu-32': (32, 28 * xm.GiB),
    'n1-highcpu-64': (64, 57 * xm.GiB),
    'n1-highcpu-96': (96, 86 * xm.GiB),
}
_A100_GPUS_TO_MACHINE_TYPE = {
    1: 'a2-highgpu-1g',
    2: 'a2-highgpu-2g',
    4: 'a2-highgpu-4g',
    8: 'a2-highgpu-8g',
    16: 'a2-megagpu-16g',
}

_CLOUD_TPU_ACCELERATOR_TYPES = {
    xm.ResourceType.TPU_V2: 'TPU_V2',
    xm.ResourceType.TPU_V3: 'TPU_V3',
}

_STATE_TO_STATUS = {
    aip_v1.JobState.JOB_STATE_SUCCEEDED: (
        local_status.LocalWorkUnitStatusEnum.COMPLETED
    ),
    aip_v1.JobState.JOB_STATE_CANCELLED: (
        local_status.LocalWorkUnitStatusEnum.CANCELLED
    ),
    aip_v1.JobState.JOB_STATE_QUEUED: (
        local_status.LocalWorkUnitStatusEnum.RUNNING
    ),
    aip_v1.JobState.JOB_STATE_PENDING: (
        local_status.LocalWorkUnitStatusEnum.RUNNING
    ),
    aip_v1.JobState.JOB_STATE_RUNNING: (
        local_status.LocalWorkUnitStatusEnum.RUNNING
    ),
    aip_v1.JobState.JOB_STATE_CANCELLING: (
        local_status.LocalWorkUnitStatusEnum.RUNNING
    ),
    aip_v1.JobState.JOB_STATE_PAUSED: (
        local_status.LocalWorkUnitStatusEnum.RUNNING
    ),
    aip_v1.JobState.JOB_STATE_FAILED: (
        local_status.LocalWorkUnitStatusEnum.FAILED
    ),
}

# Hide noisy warning regarding:
# `file_cache is unavailable when using oauth2client >= 4.0.0 or google-auth`
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

_default_client = None


def _vertex_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, local_executors.Vertex)


class Client:
  """Client class for interacting with AI Platform (Unified)."""

  def __init__(
      self, project: Optional[str] = None, location: str = _DEFAULT_LOCATION
  ) -> None:
    """Create a client.

    Args:
      project: GCP project ID.
      location: Location of the API endpoint. Defaults to region us-central1.
        https://cloud.google.com/vertex-ai/docs/reference/rest#service-endpoint
    """
    self.location = location
    self.project = project or auth.get_project_name()
    self.parent = f'projects/{self.project}/locations/{self.location}'
    aiplatform.init(project=self.project, location=self.location)

  def launch(self, name: str, jobs: Sequence[xm.Job]) -> str:
    """Launch jobs on AI Platform (Unified)."""
    pools = []
    tensorboard, output_dir = self.get_tensorboard_settings(jobs)
    for i, job in enumerate(jobs):
      executable = job.executable
      if not isinstance(
          executable, local_executables.GoogleContainerRegistryImage
      ):
        raise ValueError(
            'Executable {} has type {}. Executable must be of '
            'type GoogleContainerRegistryImage'.format(
                executable, type(executable)
            )
        )

      args = xm.merge_args(executable.args, job.args).to_list(utils.ARG_ESCAPER)
      env_vars = {**executable.env_vars, **job.env_vars}
      env = [{'name': k, 'value': v} for k, v in env_vars.items()]
      if i == 0 and job.executor.requirements.replicas > 1:
        raise ValueError(
            'The first job in a JobGroup using the Vertex AI '
            'executor cannot have requirements.replicas > 1.'
        )
      pool = aip_v1.WorkerPoolSpec(
          machine_spec=get_machine_spec(job),
          container_spec=aip_v1.ContainerSpec(
              image_uri=executable.image_path,
              args=args,
              env=env,
          ),
          replica_count=job.executor.requirements.replicas,
      )
      pools.append(pool)

    # TOOD(chenandrew): Vertex Training only allows for 4 worker pools.
    # If Vertex does not implement more worker pools, another work-around
    # is to simply put all jobs into the same worker pool as replicas.
    # Each replica would contain the same image, but would execute different
    # modules based the replica index. A disadvantage of this implementation
    # is that every replica must have the same machine_spec.
    if len(pools) > 4:
      raise ValueError(
          'Cloud Job for xm jobs {} contains {} worker types. '
          'Only 4 worker types are supported'.format(jobs, len(pools))
      )

    custom_job = aiplatform.CustomJob(
        project=self.project,
        location=self.location,
        display_name=name,
        worker_pool_specs=pools,
        base_output_dir=output_dir,
        staging_bucket=f'gs://{auth.get_bucket()}',
    )
    # TODO Vertex AI can't use TPUs with SA.
    # https://github.com/deepmind/xmanager/issues/11
    service_account = auth.get_service_account()
    for job in jobs:
      assert isinstance(job.executor, local_executors.Vertex)
      if job.executor.requirements.accelerator in xm.TpuType:
        service_account = None
        break
    custom_job.submit(
        service_account=service_account,
        tensorboard=tensorboard,
        enable_web_access=True,
    )
    print(f'Job launched at: {custom_job._dashboard_uri()}')  # pylint: disable=protected-access
    return custom_job.resource_name

  def get_tensorboard_settings(self, jobs: Sequence[xm.Job]) -> Tuple[str, str]:
    """Get the tensorboard settings for a sequence of Jobs."""
    executors = []
    for job in jobs:
      assert isinstance(job.executor, local_executors.Vertex)
      executors.append(job.executor)
    if all(not executor.tensorboard for executor in executors):
      return '', ''

    if not executors[0].tensorboard:
      raise ValueError(
          'Jobs in this job group must have the same tensorboard settings. '
          + 'jobs[0] has no tensorboard settings.'
      )
    output_dir = executors[0].tensorboard.base_output_directory
    tensorboard = executors[0].tensorboard.name
    for i, executor in enumerate(executors):
      if not executor:
        raise ValueError(
            'Jobs in this job group must have the same tensorboard settings. '
            + 'jobs[i] has no tensorboard settings.'
        )
      if (
          executor.tensorboard.name != tensorboard
          or executor.tensorboard.base_output_directory != output_dir
      ):
        raise ValueError(
            'Jobs in this job group must have the same tensorboard settings. '
            + f'jobs[0] has tensorboard = {tensorboard} and output_dir ='
            + f'{output_dir}. jobs[{i}] has tensorboard = '
            + f'{executor.tensorboard.name} and output_dir = '
            f'{executor.tensorboard.base_output_directory}.'
        )
    if output_dir and not output_dir.startswith('gs://'):
      output_dir = os.path.join('gs://', output_dir)
    return tensorboard, output_dir

  async def wait_for_job(self, job_name: str) -> None:
    job = aiplatform.CustomJob.get(job_name)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, job._block_until_complete)  # pylint: disable=protected-access

  def cancel(self, job_name: str) -> None:
    aiplatform.CustomJob.get(job_name).cancel()

  async def get_or_create_tensorboard(self, name: str) -> str:
    """Gets or creates a Vertex Tensorboard instance."""
    tensorboard_client = aip_v1.TensorboardServiceAsyncClient(
        client_options={
            'api_endpoint': f'{self.location}-aiplatform.googleapis.com'
        }
    )
    request = aip_v1.ListTensorboardsRequest(
        parent=self.parent, filter=f'displayName={name}'
    )
    response = await tensorboard_client.list_tensorboards(request)
    async for page in response.pages:
      if page.tensorboards:
        return response.tensorboards[0].name
    return await self.create_tensorboard(name)

  async def create_tensorboard(self, name: str) -> str:
    """Creates a Vertex Tensorboard instance."""
    tensorboard_client = aip_v1.TensorboardServiceAsyncClient(
        client_options={
            'api_endpoint': f'{self.location}-aiplatform.googleapis.com'
        }
    )
    tensorboard = aip_v1.Tensorboard(display_name=name)
    op = await tensorboard_client.create_tensorboard(
        aip_v1.CreateTensorboardRequest(
            parent=self.parent,
            tensorboard=tensorboard,
        )
    )
    return (await op.result()).name

  def get_state(self, job_name: str) -> aip_v1.JobState:
    return aiplatform.CustomJob.get(job_name).state


def set_default_client(client: Client) -> None:
  global _default_client
  _default_client = client


def get_default_client():
  global _default_client
  if _default_client is None:
    _default_client = Client()
  return _default_client


def get_machine_spec(job: xm.Job) -> Dict[str, Any]:
  """Get the GCP machine type that best matches the Job's requirements."""
  assert isinstance(job.executor, local_executors.Vertex)
  requirements = job.executor.requirements
  spec = {}
  for resource, value in requirements.task_requirements.items():
    accelerator_type = None
    if resource in xm.GpuType:
      accelerator_type = 'NVIDIA_TESLA_' + str(resource).upper()
    elif resource in xm.TpuType:
      accelerator_type = _CLOUD_TPU_ACCELERATOR_TYPES[resource]
    if accelerator_type:
      spec['accelerator_type'] = aip_v1.AcceleratorType[accelerator_type]
      spec['accelerator_count'] = int(value)
  accelerator = spec.get('accelerator_type', None)
  if accelerator and accelerator == aip_v1.AcceleratorType.NVIDIA_TESLA_A100:
    for gpus, machine_type in sorted(_A100_GPUS_TO_MACHINE_TYPE.items()):
      if spec['accelerator_count'] <= gpus:
        spec['machine_type'] = machine_type
        break
    if not spec.get('machine_type', None):
      raise ValueError(
          'a100={} does not fit in any valid machine type'.format(
              spec['accelerator_count']
          )
      )
  elif (
      accelerator == aip_v1.AcceleratorType.TPU_V2
      or accelerator == aip_v1.AcceleratorType.TPU_V3
  ):
    spec['machine_type'] = 'cloud-tpu'
  else:
    spec['machine_type'] = cpu_ram_to_machine_type(
        requirements.task_requirements.get(xm.ResourceType.CPU),
        requirements.task_requirements.get(xm.ResourceType.RAM),
    )
  return spec


@attr.s(auto_attribs=True)
class VertexHandle(local_execution.ExecutionHandle):
  """A handle for referring to the launched container."""

  job_name: str

  async def wait(self) -> None:
    await get_default_client().wait_for_job(self.job_name)

  def stop(self) -> None:
    get_default_client().cancel(self.job_name)

  def get_status(self) -> local_status.LocalWorkUnitStatus:
    state = get_default_client().get_state(self.job_name)
    status = _STATE_TO_STATUS[int(state)]
    return local_status.LocalWorkUnitStatus(status=status)


# Must act on all jobs with `local_executors.Vertex` executor.
def launch(
    experiment_title: str, work_unit_name: str, job_group: xm.JobGroup
) -> List[VertexHandle]:
  """Launch Vertex jobs in the job_group and return a handler."""
  jobs = xm.job_operators.collect_jobs_by_filter(
      job_group, _vertex_job_predicate
  )
  # As client creation may throw, do not initiate it if there are no jobs.
  if not jobs:
    return []

  job_name = get_default_client().launch(
      name=f'{experiment_title}_{work_unit_name}',
      jobs=jobs,
  )
  return [VertexHandle(job_name=job_name)]


def cpu_ram_to_machine_type(cpu: Optional[int], ram: Optional[int]) -> str:
  """Convert a cpu and memory spec into a machine type."""
  cpu = cpu or 0
  ram = ram or 0
  if cpu + ram == 0:
    return 'n1-standard-4'

  optimal_machine_type = ''
  optimal_excess_resources = math.inf
  for machine_type, (
      machine_cpu,
      machine_ram,
  ) in _MACHINE_TYPE_TO_CPU_RAM.items():
    if machine_cpu >= cpu and machine_ram >= ram:
      excess = machine_cpu + machine_ram - cpu - ram
      if excess < optimal_excess_resources:
        optimal_machine_type = machine_type
        optimal_excess_resources = excess

  if optimal_machine_type:
    return optimal_machine_type
  raise ValueError(
      '(cpu={}, ram={}) does not fit in any valid machine type'.format(cpu, ram)
  )
