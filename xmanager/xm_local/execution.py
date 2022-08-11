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

import abc
import asyncio
import atexit
from concurrent import futures
import os
import subprocess
import threading
from typing import Any, Awaitable, Callable, List, Optional, cast

from absl import logging
import attr
from docker.models import containers
from xmanager import xm
from xmanager.docker import docker_adapter
from xmanager.xm import job_operators
from xmanager.xm import pattern_matching
from xmanager.xm import utils
from xmanager.xm_local import executables
from xmanager.xm_local import executors
from xmanager.xm_local import status

_DEFAULT_ENCODING = 'utf-8'
_BRIDGE_NETWORK_NAME = 'xmanager'


def _print_chunk(name: str, line: str) -> None:
  print('[{}] {}'.format(name, line.strip()))


class ExecutionHandle(abc.ABC):
  """An interface for operating on executions."""

  @abc.abstractmethod
  async def wait(self) -> None:
    raise NotImplementedError

  @abc.abstractmethod
  def get_status(self) -> status.LocalWorkUnitStatus:
    """Aggregates the statuses of all jobs in the work unit into one status."""
    raise NotImplementedError


class LocalExecutionHandle(ExecutionHandle, abc.ABC):
  """An interface for operating on local executions."""

  @abc.abstractmethod
  async def monitor(self) -> None:
    raise NotImplementedError

  @abc.abstractmethod
  def terminate(self) -> None:
    raise NotImplementedError


async def _throw_on_unknown_executable(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: Any,
) -> LocalExecutionHandle:
  raise TypeError(f'Unsupported executable for local execution: {executable!r}')


@attr.s(auto_attribs=True)
class ContainerHandle(LocalExecutionHandle):
  """A handle for referring to the launched container."""

  name: str
  model: Optional[containers.Container]
  stream_output: bool
  futures_executor: futures.Executor = attr.Factory(futures.ThreadPoolExecutor)

  async def wait(self) -> None:
    if self.model is None:
      return

    response = await asyncio.wrap_future(
        self.futures_executor.submit(self.model.wait))
    status_code = response['StatusCode']
    if status_code != 0:
      raise RuntimeError(
          f'Container {self.model!r} returned non-zero status: {status_code}')

  def get_status(self) -> status.LocalWorkUnitStatus:
    raise NotImplementedError

  def terminate(self) -> None:
    if self.model is None:
      return

    self.model.stop()
    self.futures_executor.shutdown(wait=True)

  async def monitor(self) -> None:
    if self.model is None:
      return

    def _stream_chunks() -> None:
      for chunk in self.model.logs(stream=True, follow=True):
        _print_chunk(self.name, chunk.decode(_DEFAULT_ENCODING))

    if self.stream_output:
      await asyncio.wrap_future(self.futures_executor.submit(_stream_chunks))


async def _launch_loaded_container_image(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LoadedContainerImage,
) -> LocalExecutionHandle:
  """Launches a preloaded image as a detached container."""
  if not isinstance(job.executor, executors.Local):
    raise TypeError(f'Expected {job!r} to have the Local executor')
  executor = cast(executors.Local, job.executor)
  instance = docker_adapter.instance()

  if not instance.has_network(_BRIDGE_NETWORK_NAME):
    instance.create_network(_BRIDGE_NETWORK_NAME)

  gpu_count = int(
      executor.requirements.task_requirements.get(xm.ResourceType.LOCAL_GPU, 0))

  if gpu_count > 0:
    try:
      subprocess.check_output('nvidia-smi')
    except subprocess.CalledProcessError as exception:
      raise RuntimeError('No NVIDIA devices detected. Only NVIDIA'
                         'GPUs are currently supported') from exception

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
          'Default GCS path inside container overwritten by'
          '`volumes` parameter to %s', volumes[local_gcs_path])

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
  return ContainerHandle(
      name=job.name,
      model=container,
      stream_output=executor.experimental_stream_output,
  )


@attr.s(auto_attribs=True)
class BinaryHandle(LocalExecutionHandle):
  """A handle referring to the launched binary."""

  name: str
  process: asyncio.subprocess.Process  # pytype: disable=module-attr
  stream_output: bool

  async def wait(self) -> None:
    return_code = await self.process.wait()
    if return_code != 0:
      raise RuntimeError(
          f'Process {self.process!r} returned non-zero code: {return_code}')

  def get_status(self) -> status.LocalWorkUnitStatus:
    raise NotImplementedError

  def terminate(self) -> None:
    self.process.terminate()

  async def monitor(self) -> None:
    if self.stream_output:
      while True:
        line = await self.process.stdout.readline()
        if not line:
          break
        _print_chunk(self.name, line.decode(_DEFAULT_ENCODING))


async def _launch_local_binary(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LocalBinary,
) -> LocalExecutionHandle:
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
      if job.executor.experimental_stream_output else None,
      stderr=asyncio.subprocess.STDOUT
      if job.executor.experimental_stream_output else None,
  )
  return BinaryHandle(
      name=job.name,
      process=process,
      stream_output=job.executor.experimental_stream_output,
  )


# PyType infers the return type of `async` functions without wrapping them with
# `Awaitable`, so we are overriding the type of `_LOCAL_EXECUTION_ROUTER` to
# make it right.
_LocalExecutionRouter = Callable[[Callable[[str], str], xm.Job, Any],
                                 Awaitable[LocalExecutionHandle]]
_LOCAL_EXECUTION_ROUTER: _LocalExecutionRouter = pattern_matching.match(
    _launch_loaded_container_image,
    _launch_local_binary,
    _throw_on_unknown_executable,  # pytype: disable=annotation-type-mismatch
)

# Note that currently handles are never removed from the list. We can consider
# removing them on completion if needed.
_local_jobs: List[LocalExecutionHandle] = []
_local_jobs_lock = threading.Lock()


@atexit.register
def _terminate_local_jobs():
  """Terminates local jobs that were launched during the current session."""
  with _local_jobs_lock:
    if _local_jobs:
      print(f'Terminating {len(_local_jobs)} local job(s)'
            ' that may still be running...')
    for local_job in _local_jobs:
      try:
        local_job.terminate()
      except Exception:  # pylint: disable=broad-except
        logging.warning('Unable to terminate %s', repr(local_job))


def _local_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, executors.Local)


async def launch(get_full_job_name: Callable[[str], str],
                 job_group: xm.JobGroup) -> List[LocalExecutionHandle]:
  """Launches jobs with `xm_local.Local` executor."""
  # Must act on all jobs with `Local` executor.
  local_jobs = job_operators.collect_jobs_by_filter(job_group,
                                                    _local_job_predicate)
  handles: List[LocalExecutionHandle] = [
      await _LOCAL_EXECUTION_ROUTER(get_full_job_name, job, job.executable)
      for job in local_jobs
  ]
  with _local_jobs_lock:
    _local_jobs.extend(handles)
  return handles
