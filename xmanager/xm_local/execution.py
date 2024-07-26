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
"""Methods for executing job groups using the local backend."""

import asyncio
import atexit
import os
import subprocess
import threading
from typing import Callable, List, cast

from absl import logging
from xmanager import xm
from xmanager.docker import docker_adapter
from xmanager.xm import job_operators
from xmanager.xm import utils
from xmanager.xm_local import executables
from xmanager.xm_local import executors
from xmanager.xm_local import handles
from xmanager.xm_local import registry

_BRIDGE_NETWORK_NAME = 'xmanager'


async def _launch_loaded_container_image(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LoadedContainerImage,
) -> handles.LocalExecutionHandle:
  """Launches a preloaded image as a detached container."""
  if not isinstance(job.executor, executors.Local):
    raise TypeError(f'Expected {job!r} to have the Local executor')
  executor = cast(executors.Local, job.executor)
  instance = docker_adapter.instance()

  if not instance.has_network(_BRIDGE_NETWORK_NAME):
    instance.create_network(_BRIDGE_NETWORK_NAME)

  gpu_count = int(
      executor.requirements.task_requirements.get(xm.ResourceType.LOCAL_GPU, 0)
  )

  if gpu_count > 0:
    try:
      subprocess.check_output('nvidia-smi')
    except Exception as exception:
      raise RuntimeError(
          'No NVIDIA devices detected. Only NVIDIA GPUs are currently supported'
      ) from exception

  args = xm.merge_args(executable.args, job.args).to_list(utils.ARG_ESCAPER)
  env_vars = {**executable.env_vars, **job.env_vars}
  options = executor.docker_options or executors.DockerOptions()

  volumes = options.volumes or {}
  # Add GCP credentials to Local Executor.
  local_gcloud_config_path = os.path.expanduser('~/.config/gcloud')
  image_gcloud_config_path = '/root/.config/gcloud'
  volumes[local_gcloud_config_path] = image_gcloud_config_path

  if options.mount_gcs_path and os.path.isdir(os.path.expanduser('~/gcs')):
    local_gcs_path = os.path.expanduser('~/gcs')
    image_gcs_path = '/gcs'

    if local_gcs_path not in volumes:
      volumes[local_gcs_path] = image_gcs_path
    else:
      logging.warning(
          (
              'Default GCS path inside container overwritten by'
              '`volumes` parameter to %s'
          ),
          volumes[local_gcs_path],
      )

  container = instance.run_container(
      name=get_full_job_name(job.name),
      image_id=executable.image_id,
      network=_BRIDGE_NETWORK_NAME,
      args=args,
      env_vars=env_vars,
      ports=options.ports or {},
      volumes=volumes,
      gpu_count=gpu_count,
      interactive=options.interactive,
  )
  return handles.ContainerHandle(
      name=job.name,
      model=container,
      stream_output=executor.experimental_stream_output,
  )


async def _launch_local_binary(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LocalBinary,
) -> handles.LocalExecutionHandle:
  """Launches a local binary as a detached process."""
  del get_full_job_name  # Unused.
  if not isinstance(job.executor, executors.Local):
    raise TypeError(f'Expected {job!r} to have the Local executor')

  args = xm.merge_args(executable.args, job.args).to_list(utils.ARG_ESCAPER)
  env_vars = {**executable.env_vars, **job.env_vars}
  process = await asyncio.create_subprocess_exec(
      executable.path,
      *args,
      env=env_vars,
      start_new_session=True,
      stdout=asyncio.subprocess.PIPE
      if job.executor.experimental_stream_output
      else None,
      stderr=asyncio.subprocess.STDOUT
      if job.executor.experimental_stream_output
      else None,
      limit=1024 * 128,  # 128 KiB
  )
  return handles.BinaryHandle(
      name=job.name,
      process=process,
      stream_output=job.executor.experimental_stream_output,
  )


async def _local_execution_router(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: xm.Executable,
) -> handles.LocalExecutionHandle:
  match executable:
    case executables.LoadedContainerImage() as container_image:
      return await _launch_loaded_container_image(
          get_full_job_name,
          job,
          container_image,
      )
    case executables.LocalBinary() as local_binary:
      return await _launch_local_binary(get_full_job_name, job, local_binary)
    case _:
      raise TypeError(
          f'Unsupported executable for local execution: {executable!r}'
      )


# Note that currently handles are never removed from the list. We can consider
# removing them on completion if needed.
_local_jobs: List[handles.LocalExecutionHandle] = []
_local_jobs_lock = threading.Lock()


@atexit.register
def _terminate_local_jobs():
  """Terminates local jobs that were launched during the current session."""
  with _local_jobs_lock:
    if _local_jobs:
      print(
          f'Terminating {len(_local_jobs)} local job(s)'
          ' that may still be running...'
      )
    for local_job in _local_jobs:
      try:
        local_job.terminate()
      except Exception:  # pylint: disable=broad-except
        logging.warning('Unable to terminate %s', repr(local_job))


def _local_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, executors.Local)


async def launch(
    get_full_job_name: Callable[[str], str], job_group: xm.JobGroup
) -> list[handles.LocalExecutionHandle]:
  """Launches jobs with `xm_local.Local` executor."""
  # Must act on all jobs with `Local` executor.
  local_jobs = job_operators.collect_jobs_by_filter(
      job_group, _local_job_predicate
  )
  execution_handles: List[handles.LocalExecutionHandle] = [
      await _local_execution_router(get_full_job_name, job, job.executable)
      for job in local_jobs
  ]
  with _local_jobs_lock:
    _local_jobs.extend(execution_handles)
  return execution_handles


def register():
  """Registers local execution logic."""
  registry.register(
      executors.Local,
      launch=lambda local_experiment_unit, job_group: launch(
          local_experiment_unit.get_full_job_name, job_group
      ),
  )
