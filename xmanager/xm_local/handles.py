"""Local execution handles."""

import abc
import asyncio
from concurrent import futures
import logging

import attr
import docker
from docker.models import containers
from xmanager.xm_local import status


_DEFAULT_ENCODING = 'utf-8'


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

  def save_to_storage(self, experiment_id: int, work_unit_id: int) -> None:
    """Saves the handle to the local database."""
    raise NotImplementedError


class LocalExecutionHandle(ExecutionHandle, abc.ABC):
  """An interface for operating on local executions."""

  @abc.abstractmethod
  async def monitor(self) -> None:
    raise NotImplementedError

  @abc.abstractmethod
  def terminate(self) -> None:
    raise NotImplementedError


@attr.s(auto_attribs=True)
class ContainerHandle(LocalExecutionHandle):
  """A handle for referring to the launched container."""

  name: str
  model: containers.Container | None
  stream_output: bool
  futures_executor: futures.Executor = attr.Factory(futures.ThreadPoolExecutor)

  async def wait(self) -> None:
    if self.model is None:
      return

    def _wait() -> None:
      try:
        self.model.wait()
      except docker.errors.NotFound:
        logging.info(
            'Container %s not found (it may have already been removed).',
            self.model.name,
        )

    await asyncio.wrap_future(self.futures_executor.submit(_wait))

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
      try:
        for chunk in self.model.logs(stream=True, follow=True):
          _print_chunk(self.name, chunk.decode(_DEFAULT_ENCODING))
      except docker.errors.NotFound:
        logging.info(
            'Container %s not found (it may have already been removed).',
            self.model.name,
        )

    if self.stream_output:
      await asyncio.wrap_future(self.futures_executor.submit(_stream_chunks))


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
          f'Process {self.process!r} returned non-zero code: {return_code}'
      )

  def get_status(self) -> status.LocalWorkUnitStatus:
    raise NotImplementedError

  def terminate(self) -> None:
    self.process.terminate()

  async def monitor(self) -> None:
    if self.stream_output:
      if not self.process.stdout:
        raise ValueError(
            'No stdout available from process. Cannot stream output.'
        )
      while True:
        line = await self.process.stdout.readline()
        if not line:
          break
        _print_chunk(self.name, line.decode(_DEFAULT_ENCODING))
