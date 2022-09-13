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
from concurrent import futures
import time
from typing import Any, Awaitable, Callable, List, Mapping, Optional, Sequence

from absl import logging
import attr
from kubernetes import client as k8s_client
from xmanager import xm
from xmanager.cloud import kubernetes
from xmanager.cloud import vertex
from xmanager.xm import async_packager
from xmanager.xm import id_predictor
from xmanager.xm import job_operators
from xmanager.xm import pattern_matching
from xmanager.xm_local import execution as local_execution
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local import status as local_status
from xmanager.xm_local.packaging import router as packaging_router
from xmanager.xm_local.storage import database


def _throw_on_unknown_executor(job: xm.Job, executor: Any):
  raise TypeError(f'Unsupported executor: {executor!r}. Job: {job!r}')


_EXECUTOR_VALIDATOR = pattern_matching.match(
    pattern_matching.Case([xm.Job, local_executors.Local], lambda *_: None),
    pattern_matching.Case([xm.Job, local_executors.Vertex], lambda *_: None),
    pattern_matching.Case([xm.Job, local_executors.Kubernetes],
                          lambda *_: None),
    _throw_on_unknown_executor,
)


def _validate_job_group(job_group: xm.JobGroup) -> None:
  all_jobs = job_operators.flatten_jobs(job_group)
  for job in all_jobs:
    _EXECUTOR_VALIDATOR(job, job.executor)


@attr.s(auto_attribs=True)
class _LaunchResult:
  vertex_handles: List[vertex.VertexHandle]
  k8s_handles: List[kubernetes.KubernetesHandle]
  local_handles: List[local_execution.LocalExecutionHandle]


class LocalExperimentUnit(xm.ExperimentUnit):
  """Experiment unit operated by the local backend."""

  def __init__(self, experiment: 'LocalExperiment', experiment_title: str,
               create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
               args: Optional[Mapping[str, Any]],
               role: xm.ExperimentUnitRole) -> None:
    super().__init__(experiment, create_task, args, role)
    self._experiment_title = experiment_title
    self._local_execution_handles: List[
        local_execution.LocalExecutionHandle] = []
    self._non_local_execution_handles: List[
        local_execution.ExecutionHandle] = []

  async def _submit_jobs_for_execution(self,
                                       job_group: xm.JobGroup) -> _LaunchResult:
    # We are delegating the traversal of the job group to modules.
    # That improves modularity, but sacrifices the ability to make
    # cross-executor decisions.
    vertex_handles = vertex.launch(self._experiment_title,
                                   self.experiment_unit_name, job_group)
    k8s_handles = kubernetes.launch(self.get_full_job_name, job_group)
    local_handles = await local_execution.launch(self.get_full_job_name,
                                                 job_group)
    return _LaunchResult(
        vertex_handles=vertex_handles,
        k8s_handles=k8s_handles,
        local_handles=local_handles,
    )

  def _ingest_execution_handles(self, launch_result: _LaunchResult) -> None:
    self._non_local_execution_handles.extend(launch_result.vertex_handles +
                                             launch_result.k8s_handles)
    self._local_execution_handles.extend(launch_result.local_handles)

  def _monitor_local_jobs(
      self,
      local_execution_handles: Sequence[local_execution.LocalExecutionHandle]
  ) -> None:
    for handle in local_execution_handles:
      self._create_task(handle.monitor())

  async def _wait_until_complete(self) -> None:
    try:
      await asyncio.gather(*[
          handle.wait() for handle in self._local_execution_handles +
          self._non_local_execution_handles
      ])
    except RuntimeError as error:
      raise xm.ExperimentUnitFailedError(
          error, work_unit=self if isinstance(self, LocalWorkUnit) else None)

  async def wait_for_local_jobs(self, is_exit_abrupt: bool):
    if not is_exit_abrupt:
      await asyncio.gather(
          *[handle.wait() for handle in self._local_execution_handles])

  def stop(self,
           *,
           mark_as_failed: bool = False,
           mark_as_completed: bool = False,
           message: Optional[str] = None) -> None:
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

    def stop_vertex_handle(vertex_handle: vertex.VertexHandle) -> None:
      vertex_handle.stop()

    def throw_on_unknown_handle(handle: Any) -> None:
      raise TypeError(f'Unsupported handle: {handle!r}')

    handle_stopper = pattern_matching.match(
        stop_vertex_handle,
        throw_on_unknown_handle,
    )
    handles = self._non_local_execution_handles + self._local_execution_handles
    for handle in handles:
      handle_stopper(handle)

  def get_status(self) -> local_status.LocalWorkUnitStatus:
    """Gets the current status of the work unit."""
    handles = self._non_local_execution_handles + self._local_execution_handles
    if len(handles) == 1:
      return handles[0].get_status()
    raise NotImplementedError(
        'Status aggregation for work units with multiple jobs is not '
        'implemented yet.')


class LocalWorkUnit(LocalExperimentUnit):
  """A work unit operated by the local backend."""

  def __init__(self, experiment: 'LocalExperiment', experiment_title: str,
               create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
               args: Mapping[str, Any], role: xm.ExperimentUnitRole,
               work_unit_id_predictor: id_predictor.Predictor) -> None:
    super().__init__(experiment, experiment_title, create_task, args, role)
    self._work_unit_id_predictor = work_unit_id_predictor
    self._work_unit_id = self._work_unit_id_predictor.reserve_id()

  def _save_handles_to_storage(
      self, handles: Sequence[local_execution.ExecutionHandle]) -> None:
    """Saves jobs present in the handlers."""

    def save_vertex_handle(vertex_handle: vertex.VertexHandle) -> None:
      database.database().insert_vertex_job(self.experiment_id,
                                            self.work_unit_id,
                                            vertex_handle.job_name)

    def save_k8s_handle(k8s_handle: kubernetes.KubernetesHandle) -> None:
      for job in k8s_handle.jobs:
        namespace = job.metadata.namespace or 'default'
        name = job.metadata.name
        database.database().insert_kubernetes_job(self.experiment_id,
                                                  self.work_unit_id,
                                                  namespace, name)

    def throw_on_unknown_handle(handle: Any) -> None:
      raise TypeError(f'Unsupported handle: {handle!r}')

    handle_saver = pattern_matching.match(
        save_vertex_handle,
        save_k8s_handle,
        throw_on_unknown_handle,
    )
    for handle in handles:
      handle_saver(handle)

  async def _launch_job_group(
      self,
      job_group: xm.JobGroup,
      args_view: Mapping[str, Any],
      identity: str,
  ) -> None:
    del args_view  # Unused.
    _validate_job_group(job_group)

    if identity:
      raise ValueError('LocalExperiment does not support idempotent experiment '
                       'unit creation.')

    async with self._work_unit_id_predictor.submit_id(self.work_unit_id):
      launch_result = await self._submit_jobs_for_execution(job_group)
      self._ingest_execution_handles(launch_result)
      # TODO: Save the local jobs to the database as well.
      self._save_handles_to_storage(launch_result.vertex_handles +
                                    launch_result.k8s_handles)
      self._monitor_local_jobs(launch_result.local_handles)

  @property
  def experiment_unit_name(self) -> str:
    return f'{self.experiment_id}_{self._work_unit_id}'

  @property
  def work_unit_id(self) -> int:
    return self._work_unit_id


class LocalAuxiliaryUnit(LocalExperimentUnit):
  """An auxiliary unit operated by the local backend."""

  async def _launch_job_group(self, job_group: xm.JobGroup,
                              args_view: Mapping[str, Any]) -> None:
    del args_view  # Unused.
    _validate_job_group(job_group)

    launch_result = await self._submit_jobs_for_execution(job_group)
    self._ingest_execution_handles(launch_result)
    self._monitor_local_jobs(launch_result.local_handles)

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

  def _create_experiment_unit(self, args: Optional[Mapping[str, Any]],
                              role: xm.ExperimentUnitRole,
                              identity: str) -> Awaitable[xm.ExperimentUnit]:
    """Creates a new WorkUnit instance for the experiment."""
    if identity:
      raise ValueError('LocalExperiment does not support idempotent experiment '
                       'unit creation.')

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
        role: xm.AuxiliaryUnitRole) -> Awaitable[xm.ExperimentUnit]:
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

    return pattern_matching.match(create_work_unit, create_auxiliary_unit)(role)

  def _wait_for_local_jobs(self, is_exit_abrupt: bool):
    if self._experiment_units:
      print('Waiting for local jobs to complete. '
            'Press Ctrl+C to terminate them and exit')
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


def create_experiment(experiment_title: str) -> xm.Experiment:
  """Create Experiment."""
  experiment = LocalExperiment(experiment_title)
  database.database().insert_experiment(experiment.experiment_id,
                                        experiment._experiment_title)  # pylint: disable=protected-access
  return experiment


def get_experiment(experiment_id: int) -> xm.Experiment:
  """Returns an Experiment instance associated with this experiment id."""
  # pylint: disable=protected-access
  experiment_result = database.database().get_experiment(experiment_id)
  experiment = LocalExperiment(experiment_result.experiment_title)
  experiment._id = experiment_id
  experiment._work_unit_id_predictor = id_predictor.Predictor(1)
  for work_unit_result in experiment_result.work_units:
    work_unit = LocalWorkUnit(experiment, experiment_result.experiment_title,
                              lambda _: None, {}, xm.WorkUnitRole(),
                              experiment._work_unit_id_predictor)
    work_unit._work_unit_id = work_unit_result.work_unit_id
    non_local_handles = []
    kubernetes_jobs = []
    for _, data in work_unit_result.jobs.items():
      if data.HasField('local'):
        logging.warning(
            '[Experiment id: %s, work unit id: %s] '
            'Loading local experiment units from storage is not implemented.',
            experiment_id, work_unit_result.work_unit_id)
      # "caip" is the legacy field name of vertex inside the proto.
      elif data.HasField('caip'):
        non_local_handles = [vertex.VertexHandle(data.caip.resource_name)]
      elif data.HasField('kubernetes'):
        job = k8s_client.V1Job()
        job.metadata = k8s_client.V1ObjectMeta(
            namespace=data.kubernetes.namespace, name=data.kubernetes.job_name)
        kubernetes_jobs.append(job)
        non_local_handles = [kubernetes.KubernetesHandle(kubernetes_jobs)]
    work_unit._non_local_execution_handles = non_local_handles
    experiment._experiment_units.append(work_unit)
    experiment._work_unit_count += 1
  return experiment
  # pylint: enable=protected-access


def list_experiments() -> Sequence[xm.Experiment]:
  """Yields a list of Experiment instances that have been created thus far."""
  experiment_ids = database.database().list_experiment_ids()
  return [get_experiment(experiment_id) for experiment_id in experiment_ids]
