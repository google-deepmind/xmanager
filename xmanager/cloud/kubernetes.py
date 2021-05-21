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
"""Client for interacting with Kubernetes."""

import asyncio
import functools
from typing import Dict, List, Optional, Sequence

import attr
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from xmanager import xm
from xmanager.cloud import utils as cloud_utils
from xmanager.xm import resources as xm_resources
from xmanager.xm import utils
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import execution as local_execution
from xmanager.xm_local import executors as local_executors


@functools.lru_cache()
def client():
  # Global singleton defers client creation until an actual launch.
  # If the user only launches local jobs, they don't need to create a client.
  return Client()


def _kubernetes_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, local_executors.Kubernetes)


class Client:
  """Client class for interacting with Kubernetes."""

  def __init__(self, api_client: Optional[k8s_client.ApiClient] = None) -> None:
    if api_client is None:
      k8s_config.load_kube_config()
      api_client = k8s_client.ApiClient()
    self.api_client = api_client

  def launch(self, experiment_title: str, experiment_id: int, work_unit_id: int,
             jobs: Sequence[xm.Job]) -> List[k8s_client.V1Job]:
    """Launches jobs on Kubernetes."""
    batch_jobs = []
    service = f'{experiment_title}-{experiment_id}'
    domains = [
        f'workerpool{i}.{service}.default.svc.cluster.local:2222'
        for i in range(len(jobs))
    ]
    specs = cloud_utils.create_cluster_specs(domains)
    for i, job in enumerate(jobs):
      executable = job.executable
      executor = job.executor
      if not isinstance(executable,
                        local_executables.GoogleContainerRegistryImage):
        raise ValueError('Executable {} has type {}. Executable must be of '
                         'type GoogleContainerRegistryImage.'.format(
                             executable, type(executable)))
      env = [
          k8s_client.V1EnvVar(k, v) for k, v in {
              **executable.env_vars,
              **job.env_vars
          }.items()
      ]
      env.append(k8s_client.V1EnvVar('CLUSTER_SPEC', specs[i]))
      container = k8s_client.V1Container(
          # TODO: Replace with xm.Job identity.
          name='container',
          image=executable.image_path,
          resources=resources_from_executor(executor),
          args=utils.to_command_line_args(
              xm.merge_args(executable.args, job.args)),
          env=env)
      k8s_job = k8s_client.V1Job()
      # TODO: Replace with xm.Job identity.
      k8s_job.metadata = k8s_client.V1ObjectMeta(
          name=f'{experiment_title}.{experiment_id}.{work_unit_id}.{i}')
      k8s_job.spec = k8s_client.V1JobSpec(
          template=k8s_client.V1PodTemplateSpec(
              metadata=k8s_client.V1ObjectMeta(
                  labels={'experiment': service},
                  annotations=annotations_from_executor(executor),
              ),
              spec=k8s_client.V1PodSpec(
                  hostname=f'workerpool{i}',
                  subdomain=service,
                  restart_policy='Never',
                  containers=[container],
                  node_selector=node_selector_from_executor(executor),
              ),
          ),
          backoff_limit=0,
      )
      batch_jobs.append(k8s_job)

    self._create_service(service)
    batch_api = k8s_client.BatchV1Api(self.api_client)
    for k8s_job in batch_jobs:
      batch_api.create_namespaced_job(namespace='default', body=k8s_job)
    return batch_jobs

  def _create_service(self, service: str) -> None:
    """Creates a K8s service with an `experiment: service` selector."""
    body = k8s_client.V1Service(
        metadata=k8s_client.V1ObjectMeta(name=service),
        spec=k8s_client.V1ServiceSpec(
            selector={'experiment': service},
            cluster_ip='None',
        ),
    )
    core_api = k8s_client.CoreV1Api(self.api_client)
    core_api.create_namespaced_service(namespace='default', body=body)

  async def wait_for_job(self, job: k8s_client.V1Job) -> None:
    batch_api = k8s_client.BatchV1Api(self.api_client)
    backoff = 5  # seconds
    while True:
      await asyncio.sleep(backoff)
      response = batch_api.read_namespaced_job_status(
          namespace='default', name=job.metadata.name)
      if response.status.completion_time:
        return


@attr.s(auto_attribs=True)
class KubernetesHandle(local_execution.ExecutionHandle):
  """A handle for referring to the launched container."""

  jobs: List[k8s_client.V1Job]

  async def wait(self) -> None:
    await asyncio.gather(*[client().wait_for_job(job) for job in self.jobs])


# Must act on all jobs with `local_executors.Kubernetes` executor.
def launch(experiment_title: str, experiment_id: int, work_unit_id: int,
           job_group: xm.JobGroup) -> List[KubernetesHandle]:
  """Launch K8s jobs in the job_group and return a handler."""
  jobs = utils.collect_jobs_by_filter(job_group, _kubernetes_job_predicate)
  # As client creation may throw, do not initiate it if there are no jobs.
  if not jobs:
    return []
  k8_jobs = client().launch(
      experiment_title=experiment_title,
      experiment_id=experiment_id,
      work_unit_id=work_unit_id,
      jobs=jobs,
  )
  return [KubernetesHandle(jobs=k8_jobs)]


def resources_from_executor(
    executor: local_executors.Kubernetes) -> k8s_client.V1ResourceRequirements:
  """Get resource limits from the executor."""
  limits = {}
  for resource, value in executor.resources.task_requirements.items():
    if xm_resources.is_gpu(resource):
      # TODO: Implement detection of whether an accelerator is an Nvidia
      # GPU. amd.com/gpu is another type of GPU that is not present in GCP.
      limits['nvidia.com/gpu'] = str(value)
    elif xm_resources.is_tpu(resource):
      pass
    else:
      # Converts resource amount to a string accepted by Kubernetes:
      # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
      # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
      limits[str(resource).lower()] = str(value)

  return k8s_client.V1ResourceRequirements(limits=limits)


def annotations_from_executor(
    executor: local_executors.Kubernetes) -> Dict[str, str]:
  """Get Pod annotations from the executor for TPUs."""
  if executor.cloud_provider != local_executors.GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER:
    return {}

  if executor.resources.is_tpu_job:
    tpu_runtime_version = 'nightly'
    if executor.tpu_capability:
      tpu_runtime_version = executor.tpu_capability.tpu_runtime_version
    return {'tf-version.cloud-tpus.google.com': tpu_runtime_version}
  return {}


def node_selector_from_executor(
    executor: local_executors.Kubernetes) -> Dict[str, str]:
  """Get Pod annotations from the executor for TPUs."""
  if executor.cloud_provider != local_executors.GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER:
    return {}

  for resource in executor.resources.task_requirements:
    if xm_resources.is_gpu(resource):
      return {
          'cloud.google.com/gke-accelerator':
              'nvidia-tesla-' + str(resource).lower()
      }
  return {}
