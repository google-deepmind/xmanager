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
from typing import Callable, Dict, List, Optional, Sequence

from absl import flags
import attr
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from xmanager import xm
from xmanager.xm import utils
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import execution as local_execution
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local import status as local_status


_K8S_SERVICE_ACCOUNT_NAME = flags.DEFINE_string(
    'xm_k8s_service_account_name',
    'default',
    (
        'Specifies the Kubernetes Service Account name to be used by XManager'
        ' inthe pod specifications.'
    ),
)


@functools.lru_cache()
def client():
  # Global singleton defers client creation until an actual launch.
  # If the user only launches local jobs, they don't need to create a client.
  return Client()


def _kubernetes_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, local_executors.Kubernetes)


def convert_to_valid_label(label: str) -> str:
  """Kubernetes labels must be RFC 1123 format compliant.

  https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/apps/validation/validation.go
  A lowercase RFC 1123 label must consist of lower case alphanumeric characters
  or '-', and must start and end with an alphanumeric character (e.g. 'my-name',
  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')

  Args:
    label: Label that may or may not be RFC 1123 compliant.

  Returns:
    A RFC 1123 format compliant label.
  """
  return label.replace('_', '-')


def _load_k8s_config() -> None:
  """Loads K8s config based on where the client is running (inside cluster or not)."""
  try:
    k8s_config.load_incluster_config()
  except k8s_config.ConfigException:
    # Client is not running inside cluster
    k8s_config.load_kube_config()


class Client:
  """Client class for interacting with Kubernetes."""

  def __init__(self, api_client: Optional[k8s_client.ApiClient] = None) -> None:
    if api_client is None:
      _load_k8s_config()

      api_client = k8s_client.ApiClient()
    self.api_client = api_client

  def launch(
      self,
      get_full_job_name: Callable[[str], str],
      jobs: Sequence[xm.Job],
  ) -> List[k8s_client.V1Job]:
    """Launches jobs on Kubernetes."""
    batch_jobs = []
    service = 'experiments'
    for job in jobs:
      executable = job.executable
      executor = job.executor
      if not isinstance(
          executable, local_executables.GoogleContainerRegistryImage
      ):
        raise ValueError(
            'Executable {} has type {}. Executable must be of '
            'type GoogleContainerRegistryImage.'.format(
                executable, type(executable)
            )
        )
      all_env_vars = {**executable.env_vars, **job.env_vars}
      env = [k8s_client.V1EnvVar(k, v) for k, v in all_env_vars.items()]
      job_name = convert_to_valid_label(get_full_job_name(job.name))
      container = k8s_client.V1Container(
          name=job_name,
          image=executable.image_path,
          resources=requirements_from_executor(executor),
          args=xm.merge_args(executable.args, job.args).to_list(
              utils.ARG_ESCAPER
          ),
          env=env,
      )
      k8s_job = k8s_client.V1Job()
      k8s_job.metadata = k8s_client.V1ObjectMeta(name=job_name)
      k8s_job.spec = k8s_client.V1JobSpec(
          template=k8s_client.V1PodTemplateSpec(
              metadata=k8s_client.V1ObjectMeta(
                  labels={'service': service},
                  annotations=annotations_from_executor(executor),
              ),
              spec=k8s_client.V1PodSpec(
                  service_account=_K8S_SERVICE_ACCOUNT_NAME.value,
                  hostname=job_name,
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
    """Creates a K8s service with an `service: {service}` selector."""
    core_api = k8s_client.CoreV1Api(self.api_client)
    body = k8s_client.V1Service(
        metadata=k8s_client.V1ObjectMeta(name=service),
        spec=k8s_client.V1ServiceSpec(
            selector={'service': service},
            cluster_ip='None',
        ),
    )
    response = core_api.list_namespaced_service(namespace='default')
    for item in response.items:
      # service already exists
      if item.metadata.name == service:
        return
    core_api.create_namespaced_service(namespace='default', body=body)

  async def wait_for_job(self, job: k8s_client.V1Job) -> None:
    batch_api = k8s_client.BatchV1Api(self.api_client)
    backoff = 5  # seconds
    while True:
      await asyncio.sleep(backoff)
      response = batch_api.read_namespaced_job_status(
          namespace='default', name=job.metadata.name
      )
      if response.status.completion_time:
        return


@attr.s(auto_attribs=True)
class KubernetesHandle(local_execution.ExecutionHandle):
  """A handle for referring to the launched container."""

  jobs: List[k8s_client.V1Job]

  async def wait(self) -> None:
    await asyncio.gather(*[client().wait_for_job(job) for job in self.jobs])

  def get_status(self) -> local_status.LocalWorkUnitStatus:
    raise NotImplementedError


# Must act on all jobs with `local_executors.Kubernetes` executor.
def launch(
    get_full_job_name: Callable[[str], str], job_group: xm.JobGroup
) -> List[KubernetesHandle]:
  """Launch K8s jobs in the job_group and return a handler."""
  jobs = xm.job_operators.collect_jobs_by_filter(
      job_group, _kubernetes_job_predicate
  )
  # As client creation may throw, do not initiate it if there are no jobs.
  if not jobs:
    return []
  k8_jobs = client().launch(
      get_full_job_name=get_full_job_name,
      jobs=jobs,
  )
  return [KubernetesHandle(jobs=k8_jobs)]


def requirements_from_executor(
    executor: local_executors.Kubernetes,
) -> k8s_client.V1ResourceRequirements:
  """Get resource limits from the executor."""
  limits = {}
  for resource, value in executor.requirements.task_requirements.items():
    if resource in xm.GpuType:
      # TODO: Implement detection of whether an accelerator is an Nvidia
      # GPU. amd.com/gpu is another type of GPU that is not present in GCP.
      limits['nvidia.com/gpu'] = f'{value:g}'
    elif resource in xm.TpuType:
      pass
    else:
      # Converts resource amount to a string accepted by Kubernetes:
      # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
      # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
      limits[str(resource).lower()] = f'{value:.15g}'

  return k8s_client.V1ResourceRequirements(limits=limits)


def annotations_from_executor(
    executor: local_executors.Kubernetes,
) -> Dict[str, str]:
  """Get Pod annotations from the executor for TPUs."""
  if (
      executor.cloud_provider
      != local_executors.GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER
  ):
    return {}

  if executor.requirements.accelerator in xm.TpuType:
    tpu_runtime_version = 'nightly'
    if executor.tpu_capability:
      tpu_runtime_version = executor.tpu_capability.tpu_runtime_version
    return {'tf-version.cloud-tpus.google.com': tpu_runtime_version}
  return {}


def node_selector_from_executor(
    executor: local_executors.Kubernetes,
) -> Dict[str, str]:
  """Get Pod annotations from the executor for TPUs."""
  if (
      executor.cloud_provider
      != local_executors.GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER
  ):
    return {}

  for resource in executor.requirements.task_requirements:
    if resource in xm.GpuType:
      return {
          'cloud.google.com/gke-accelerator': (
              'nvidia-tesla-' + str(resource).lower()
          )
      }
  return {}
