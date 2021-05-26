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
import threading
from typing import Any, Awaitable, Callable, List, cast

from absl import logging
import attr
from docker.models import containers
from xmanager import xm
from xmanager.docker import docker_adapter
from xmanager.xm import pattern_matching
from xmanager.xm import utils
from xmanager.xm_local import executables
from xmanager.xm_local import executors


class ExecutionHandle(abc.ABC):
  """An interface for operating on executions."""

  @abc.abstractmethod
  async def wait(self) -> None:
    raise NotImplementedError


class LocalExecutionHandle(ExecutionHandle, abc.ABC):
  """An interface for operating on local executions."""

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

  model: containers.Container

  async def wait(self) -> None:
    with futures.ThreadPoolExecutor() as executor:
      response = await asyncio.wrap_future(executor.submit(self.model.wait))
      status_code = response['StatusCode']
      if status_code != 0:
        raise RuntimeError(
            f'Container {self.model!r} returned non-zero status: {status_code}')

  def terminate(self) -> None:
    self.model.stop()


async def _launch_loaded_container_image(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LoadedContainerImage,
) -> LocalExecutionHandle:
  """Launches a preloaded image as a detached container."""
  assert isinstance(job.executor, executors.Local)
  executor = cast(executors.Local, job.executor)

  args = utils.to_command_line_args(xm.merge_args(executable.args, job.args))
  env_vars = {**executable.env_vars, **job.env_vars}
  options = executor.docker_options or executors.DockerOptions()

  container = docker_adapter.instance().run_container(
      name=get_full_job_name(job.name),
      image_id=executable.image_id,
      args=args,
      env_vars=env_vars,
      ports=options.ports or {},
      volumes=options.volumes or {},
  )
  return ContainerHandle(model=container)


@attr.s(auto_attribs=True)
class BinaryHandle(LocalExecutionHandle):
  """A handle referring to the launched binary."""

  process: asyncio.subprocess.Process

  async def wait(self) -> None:
    return_code = await self.process.wait()
    if return_code != 0:
      raise RuntimeError(
          f'Process {self.process!r} returned non-zero code: {return_code}')

  def terminate(self) -> None:
    self.process.terminate()


async def _launch_local_binary(
    get_full_job_name: Callable[[str], str],
    job: xm.Job,
    executable: executables.LocalBinary,
) -> LocalExecutionHandle:
  """Launches a local binary as a detached process."""
  del get_full_job_name  # Unused.
  args = utils.to_command_line_args(xm.merge_args(executable.args, job.args))
  env_vars = {**executable.env_vars, **job.env_vars}
  process = await asyncio.create_subprocess_exec(
      executable.path, *args, env=env_vars, start_new_session=True)
  return BinaryHandle(process=process)


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
        logging.warn('Unable to terminate %s', repr(local_job))


def _local_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, executors.Local)


async def launch(get_full_job_name: Callable[[str], str],
                 job_group: xm.JobGroup) -> List[LocalExecutionHandle]:
  """Launches jobs with `xm_local.Local` executor."""
  # Must act on all jobs with `Local` executor.
  local_jobs = utils.collect_jobs_by_filter(
      job_group,
      _local_job_predicate,
  )
  handles: List[LocalExecutionHandle] = [
      await _LOCAL_EXECUTION_ROUTER(get_full_job_name, job, job.executable)
      for job in local_jobs
  ]
  with _local_jobs_lock:
    _local_jobs.extend(handles)
  return handles
