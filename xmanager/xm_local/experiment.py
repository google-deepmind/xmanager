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
"""Implementation of the local scheduler experiment."""

import asyncio
import collections
from concurrent import futures
import time
from typing import Any, Awaitable, Callable, List, Mapping, Optional, Sequence, Type

from absl import logging
import attr
from xmanager import xm
from xmanager.xm import async_packager
from xmanager.xm import id_predictor
from xmanager.xm import job_operators
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local import handles as execution_handles
from xmanager.xm_local import registry
from xmanager.xm_local import status as local_status
from xmanager.xm_local.packaging import router as packaging_router
from xmanager.xm_local.storage import database


def _validate_job_group(job_group: xm.JobGroup) -> None:
  all_jobs = job_operators.flatten_jobs(job_group)
  for job in all_jobs:
    if not registry.is_registered(type(job.executor)):
      raise TypeError(f'Unsupported executor: {job.executor!r}. Job: {job!r}')


@attr.s(auto_attribs=True)
class _LaunchResult:
  handles: collections.defaultdict[Type[xm.Executor], list[Any]]

  def get_local_handles(self) -> list[execution_handles.LocalExecutionHandle]:
    return self.handles.get(local_executors.Local, [])

  def get_non_local_handles(self) -> list[execution_handles.ExecutionHandle]:
    return self.handles.get(local_executors.Vertex, []) + self.handles.get(
        local_executors.Kubernetes, []
    )


class LocalExperimentUnit(xm.ExperimentUnit):
  """Experiment unit operated by the local backend."""

  def __init__(
      self,
      experiment: 'LocalExperiment',
      experiment_title: str,
      create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
      args: Optional[Mapping[str, Any]],
      role: xm.ExperimentUnitRole,
  ) -> None:
    super().__init__(experiment, create_task, args, role)
    self._experiment_title = experiment_title
    self._local_execution_handles: List[
        execution_handles.LocalExecutionHandle
    ] = []
    self._non_local_execution_handles: List[
        execution_handles.ExecutionHandle
    ] = []

  async def _submit_jobs_for_execution(
      self, job_group: xm.JobGroup
  ) -> _LaunchResult:
    # We are delegating the traversal of the job group to modules.
    # That improves modularity, but sacrifices the ability to make
    # cross-executor decisions.
    executor_types = set()
    for job in job_operators.flatten_jobs(job_group):
      executor_types.add(type(job.executor))

    handles = collections.defaultdict(list)
    for executor_type in executor_types:
      handles[executor_type] = await executor_type.launch(
          local_experiment_unit=self, job_group=job_group
      )

    return _LaunchResult(handles)

  def _ingest_execution_handles(self, launch_result: _LaunchResult) -> None:
    self._non_local_execution_handles.extend(
        launch_result.get_non_local_handles()
    )
    self._local_execution_handles.extend(launch_result.get_local_handles())

  def _monitor_local_jobs(
      self,
      local_execution_handles: Sequence[execution_handles.LocalExecutionHandle],
  ) -> None:
    for handle in local_execution_handles:
      self._create_task(handle.monitor())

  async def _wait_until_complete(self) -> None:
    try:
      await asyncio.gather(
          *[
              handle.wait()
              for handle in self._local_execution_handles
              + self._non_local_execution_handles
          ]
      )
    except RuntimeError as error:
      raise xm.ExperimentUnitFailedError(
          error, work_unit=self if isinstance(self, LocalWorkUnit) else None
      )

  async def wait_for_local_jobs(self, is_exit_abrupt: bool):
    if not is_exit_abrupt:
      await asyncio.gather(
          *[handle.wait() for handle in self._local_execution_handles]
      )

  def stop(
      self,
      *,
      mark_as_failed: bool = False,
      mark_as_completed: bool = False,
      message: Optional[str] = None,
  ) -> None:
    """Initiate the process to stop the work unit from running.

    This method will synchronously make a request for the work unit to stop.
    However, the method does not actually wait for the work unit to be in a
    terminal state.

    Use self.wait_until_complete() after self.stop() to guarantee the work unit
    is stopped.

    Args:
      mark_as_failed: Mark this unit as failed rather than stopped.
      mark_as_completed: Mark this unit as completed rather than stopped.
      message: Optional user-defined status message.
    """
    del mark_as_failed  # Not implemented in xm_local.
    del mark_as_completed  # Not implemented in xm_local.
    del message  # Not implemented in xm_local.

    handles = self._non_local_execution_handles + self._local_execution_handles
    for handle in handles:
      handle.stop()

  def get_status(self) -> local_status.LocalWorkUnitStatus:
    """Gets the current status of the work unit."""
    handles = self._non_local_execution_handles + self._local_execution_handles
    if len(handles) == 1:
      return handles[0].get_status()
    raise NotImplementedError(
        'Status aggregation for work units with multiple jobs is not '
        'implemented yet.'
    )


class LocalWorkUnit(LocalExperimentUnit):
  """A work unit operated by the local backend."""

  def __init__(
      self,
      experiment: 'LocalExperiment',
      experiment_title: str,
      create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
      args: Mapping[str, Any],
      role: xm.ExperimentUnitRole,
      work_unit_id_predictor: id_predictor.Predictor,
  ) -> None:
    super().__init__(experiment, experiment_title, create_task, args, role)
    self._work_unit_id_predictor = work_unit_id_predictor
    self._work_unit_id = self._work_unit_id_predictor.reserve_id()

  def _save_handles_to_storage(
      self, handles: Sequence[execution_handles.ExecutionHandle]
  ) -> None:
    """Saves jobs present in the handlers."""
    for handle in handles:
      handle.save_to_storage(self.experiment_id, self.work_unit_id)

  async def _launch_job_group(
      self,
      job_group: xm.JobGroup,
      args_view: Mapping[str, Any],
      identity: str,
  ) -> None:
    del args_view  # Unused.
    _validate_job_group(job_group)

    if identity:
      logging.warning(
          'LocalExperiment does not support idempotent experiment '
          'unit creation.'
      )

    async with self._work_unit_id_predictor.submit_id(self.work_unit_id):
      launch_result = await self._submit_jobs_for_execution(job_group)
      self._ingest_execution_handles(launch_result)
      # TODO: Save the local jobs to the database as well.
      self._save_handles_to_storage(launch_result.get_non_local_handles())
      self._monitor_local_jobs(launch_result.get_local_handles())

  @property
  def experiment_unit_name(self) -> str:
    return f'{self.experiment_id}_{self._work_unit_id}'

  @property
  def work_unit_id(self) -> int:
    return self._work_unit_id


class LocalAuxiliaryUnit(LocalExperimentUnit):
  """An auxiliary unit operated by the local backend."""

  async def _launch_job_group(
      self,
      job_group: xm.JobGroup,
      args_view: Mapping[str, Any],
      identity: str,
  ) -> None:
    del args_view  # Unused.
    _validate_job_group(job_group)

    if identity:
      raise ValueError(
          'LocalExperiment does not support idempotent experiment '
          'unit creation.'
      )

    launch_result = await self._submit_jobs_for_execution(job_group)
    self._ingest_execution_handles(launch_result)
    self._monitor_local_jobs(launch_result.get_local_handles())

  @property
  def experiment_unit_name(self) -> str:
    return f'{self.experiment_id}_auxiliary'


class LocalExperiment(xm.Experiment):
  """Experiment contains a family of jobs that run with the local scheduler."""

  _id: int
  _experiment_title: str
  _experiment_units: List[LocalExperimentUnit]
  _work_unit_count: int
  _async_packager = async_packager.AsyncPackager(packaging_router.package)

  def __init__(self, experiment_title: str) -> None:
    super().__init__()
    # To distinguish local job names until we use a local database generator.
    self._id = int(time.time() * 10**3)
    self._experiment_title = experiment_title
    self._experiment_units = []
    self._work_unit_count = 0

  def _create_experiment_unit(
      self,
      args: Optional[Mapping[str, Any]],
      role: xm.ExperimentUnitRole,
      identity: str,
  ) -> Awaitable[xm.ExperimentUnit]:
    """Creates a new WorkUnit instance for the experiment."""
    if identity:
      logging.warning(
          'LocalExperiment does not support idempotent experiment '
          'unit creation.'
      )

    def create_work_unit(role: xm.WorkUnitRole) -> Awaitable[xm.ExperimentUnit]:
      work_unit = LocalWorkUnit(
          self,
          self._experiment_title,
          self._create_task,
          args,
          role,
          self._work_unit_id_predictor,
      )
      self._experiment_units.append(work_unit)
      self._work_unit_count += 1
      database.database().insert_work_unit(
          self.experiment_id,
          work_unit.work_unit_id,
      )
      future = asyncio.Future()
      future.set_result(work_unit)
      return future

    # TODO: Support `role.termination_delay_secs`.
    def create_auxiliary_unit(
        role: xm.AuxiliaryUnitRole,
    ) -> Awaitable[xm.ExperimentUnit]:
      auxiliary_unit = LocalAuxiliaryUnit(
          self,
          self._experiment_title,
          self._create_task,
          args,
          role,
      )
      self._experiment_units.append(auxiliary_unit)
      future = asyncio.Future()
      future.set_result(auxiliary_unit)
      return future

    match role:
      case xm.WorkUnitRole() as role:
        return create_work_unit(role)
      case xm.AuxiliaryUnitRole() as role:
        return create_auxiliary_unit(role)
      case _:
        raise TypeError(f'Unsupported role: {role!r}')

  def _wait_for_local_jobs(self, is_exit_abrupt: bool):
    if self._experiment_units:
      print(
          'Waiting for local jobs to complete. '
          'Press Ctrl+C to terminate them and exit'
      )
    for unit in self._experiment_units:
      self._create_task(unit.wait_for_local_jobs(is_exit_abrupt))

  def __exit__(self, exc_type, exc_value, traceback):
    # Flush `.add` calls.
    self._wait_for_tasks()
    self._wait_for_local_jobs(exc_value is not None)
    return super().__exit__(exc_type, exc_value, traceback)

  async def __aexit__(self, exc_type, exc_value, traceback):
    # Flush `.add` calls.
    await self._await_for_tasks()
    self._wait_for_local_jobs(exc_value is not None)
    return await super().__aexit__(exc_type, exc_value, traceback)

  @property
  def experiment_id(self) -> int:
    return self._id

  @property
  def work_unit_count(self) -> int:
    return self._work_unit_count

  @property
  def work_units(self) -> Mapping[int, LocalExperimentUnit]:
    """Gets work units created via self.add()."""
    raise NotImplementedError

  def _get_experiment_unit(
      self,
      experiment_id: int,
      identity: str,
      role: xm.ExperimentUnitRole,
      args: Optional[Mapping[str, Any]] = None,
  ) -> Awaitable[xm.ExperimentUnit]:
    """Returns an existing experiment unit by identity.

    Args:
      experiment_id: The ID of the experiment to get the Experiment Unit for.
      identity: The identity of the Experiment Unit toget.
      role: Executable unit role: whether to fetch a work unit or auxiliary
        unit.
      args: Keyword arguments to be passed to the job.

    Returns:
      An awaitable which fetches the work unit.
    """
    raise NotImplementedError

  def _should_reload_experiment_unit(self, role: xm.ExperimentUnitRole) -> bool:
    """Returns True if the Work Unit should be reloaded based on its role.

    Args:
      role: Executable unit role trying to be reloaded.
    """
    # Since reloading isn't supported locally, we always return False.
    return False


def create_experiment(experiment_title: str) -> xm.Experiment:
  """Create Experiment."""
  experiment = LocalExperiment(experiment_title)
  database.database().insert_experiment(
      experiment.experiment_id, experiment._experiment_title  # pylint: disable=protected-access
  )
  return experiment


def get_experiment(experiment_id: int) -> xm.Experiment:
  """Returns an Experiment instance associated with this experiment id."""
  # pylint: disable=protected-access
  experiment_result = database.database().get_experiment(experiment_id)
  experiment = LocalExperiment(experiment_result.experiment_title)
  experiment._id = experiment_id
  experiment._work_unit_id_predictor = id_predictor.Predictor(1)
  for work_unit_result in experiment_result.work_units:
    work_unit = LocalWorkUnit(
        experiment,
        experiment_result.experiment_title,
        lambda _: None,
        {},
        xm.WorkUnitRole(),
        experiment._work_unit_id_predictor,
    )
    work_unit._work_unit_id = work_unit_result.work_unit_id
    non_local_handles = []
    kubernetes_jobs = []
    for _, data in work_unit_result.jobs.items():
      if data.HasField('local'):
        logging.warning(
            (
                '[Experiment id: %s, work unit id: %s] Loading local experiment'
                ' units from storage is not implemented.'
            ),
            experiment_id,
            work_unit_result.work_unit_id,
        )
      # "caip" is the legacy field name of vertex inside the proto.
      elif data.HasField('caip'):
        handle = registry.get_create_handle_method(local_executors.Vertex)(
            data=data
        )
        assert handle
        non_local_handles = [handle]
      elif data.HasField('kubernetes'):
        handle = registry.get_create_handle_method(local_executors.Kubernetes)(
            data=data, kubernetes_jobs=kubernetes_jobs
        )
        assert handle
        kubernetes_jobs = handle.kubernetes_jobs
        non_local_handles = [handle]
    work_unit._non_local_execution_handles = non_local_handles
    experiment._experiment_units.append(work_unit)
    experiment._work_unit_count += 1
  return experiment
  # pylint: enable=protected-access


def list_experiments() -> Sequence[xm.Experiment]:
  """Yields a list of Experiment instances that have been created thus far."""
  experiment_ids = database.database().list_experiment_ids()
  return [get_experiment(experiment_id) for experiment_id in experiment_ids]
