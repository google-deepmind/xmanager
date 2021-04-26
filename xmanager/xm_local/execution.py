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
from concurrent import futures
from typing import Any, Awaitable, Callable, List

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


async def _throw_on_unknown_executable(
    job: xm.Job,
    executable: Any,
) -> ExecutionHandle:
  raise TypeError(f'Unsupported executable for local execution: {executable!r}')


@attr.s(auto_attribs=True)
class ContainerHandle(ExecutionHandle):
  """A handle for referring to the launched container."""

  model: containers.Container

  async def wait(self) -> None:
    with futures.ThreadPoolExecutor() as executor:
      response = await asyncio.wrap_future(executor.submit(self.model.wait))
      status_code = response['StatusCode']
      if status_code != 0:
        raise RuntimeError(
            f'Container {self.model!r} returned non-zero status: {status_code}')


async def _launch_loaded_container_image(
    job: xm.Job,
    executable: executables.LoadedContainerImage,
) -> ExecutionHandle:
  """Launches a preloaded image as a detached container."""
  args = utils.to_command_line_args(xm.merge_args(executable.args, job.args))
  env_vars = {**executable.env_vars, **job.env_vars}
  container = docker_adapter.instance().run_container(
      image_id=executable.image_id,
      args=args,
      env_vars=env_vars,
  )
  return ContainerHandle(model=container)


@attr.s(auto_attribs=True)
class BinaryHandle(ExecutionHandle):
  """A handle referring to the launched binary."""

  process: asyncio.subprocess.Process

  async def wait(self) -> None:
    return_code = await self.process.wait()
    if return_code != 0:
      raise RuntimeError(
          f'Process {self.process!r} returned non-zero code: {return_code}')


async def _launch_local_binary(
    job: xm.Job,
    executable: executables.LocalBinary,
) -> ExecutionHandle:
  """Launches a local binary as a detached process."""
  args = [
      *utils.to_command_line_args(executable.args),
      *utils.to_command_line_args(job.args)
  ]
  env_vars = {**executable.env_vars, **job.env_vars}
  process = await asyncio.create_subprocess_exec(
      executable.path, *args, env=env_vars, start_new_session=True)
  return BinaryHandle(process=process)


# PyType infers the return type of `async` functions without wrapping them with
# `Awaitable`, so we are overriding the type of `_LOCAL_EXECUTION_ROUTER` to
# make it right.
_LocalExecutionRouter = Callable[[xm.Job, Any], Awaitable[ExecutionHandle]]
_LOCAL_EXECUTION_ROUTER: _LocalExecutionRouter = pattern_matching.match(
    _launch_loaded_container_image,
    _launch_local_binary,
    _throw_on_unknown_executable,  # pytype: disable=annotation-type-mismatch
)


def _local_job_predicate(job: xm.Job) -> bool:
  return isinstance(job.executor, executors.Local)


async def launch(job_group: xm.JobGroup) -> List[ExecutionHandle]:
  # Must act on all jobs with `Local` executor.
  local_jobs = utils.collect_jobs_by_filter(
      job_group,
      _local_job_predicate,
  )
  return [
      await _LOCAL_EXECUTION_ROUTER(job, job.executable) for job in local_jobs
  ]
