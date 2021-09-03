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
from typing import Any, Awaitable, Callable, List, Mapping, Sequence

from xmanager import xm
from xmanager.cloud import caip
from xmanager.cloud import kubernetes
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
    pattern_matching.Case([xm.Job, local_executors.Caip], lambda *_: None),
    pattern_matching.Case([xm.Job, local_executors.Kubernetes],
                          lambda *_: None),
    _throw_on_unknown_executor,
)


def _validate_job_group(job_group: xm.JobGroup) -> None:
  all_jobs = job_operators.flatten_jobs(job_group)
  for job in all_jobs:
    _EXECUTOR_VALIDATOR(job, job.executor)


class LocalWorkUnit(xm.WorkUnit):
  """WorkUnit operated by the local backend."""

  def __init__(self, experiment: 'LocalExperiment', experiment_title: str,
               work_unit_id_predictor: id_predictor.Predictor,
               create_task: Callable[[Awaitable[Any]], futures.Future],
               args: Mapping[str, Any]) -> None:
    super().__init__(experiment, work_unit_id_predictor, create_task, args)
    self._experiment_title = experiment_title
    self._local_execution_handles: List[
        local_execution.LocalExecutionHandle] = []
    self._non_local_execution_handles: List[
        local_execution.ExecutionHandle] = []

  async def _launch_job_group(self, job_group: xm.JobGroup,
                              args_view: Mapping[str, Any]) -> None:
    del args_view  # Unused.
    _validate_job_group(job_group)
    # We are delegating the traversal of the job group to modules. That improves
    # modularity, but sacrifices the ability to make cross-executor decisions.
    async with self._work_unit_id_predictor.submit_id(self.work_unit_id):
      caip_handles = caip.launch(self._experiment_title, self.work_unit_name,
                                 job_group)
      k8s_handles = kubernetes.launch(
          str(self.experiment_id), self.get_full_job_name, job_group)
      self._non_local_execution_handles.extend(caip_handles + k8s_handles)
      self._save_handles_to_storage(caip_handles + k8s_handles)
      # TODO Save the local jobs to database.
      local_handles = await local_execution.launch(self.get_full_job_name,
                                                   job_group)
      for handle in local_handles:
        self._create_task(handle.monitor())
      self._local_execution_handles.extend(local_handles)

  def _save_handles_to_storage(
      self, handles: Sequence[local_execution.ExecutionHandle]) -> None:
    """Saves jobs present in the handlers."""

    def save_caip_handle(caip_handle: caip.CaipHandle) -> None:
      database.database().insert_caip_job(self.experiment_id, self.work_unit_id,
                                          self.work_unit_name,
                                          caip_handle.job_name)

    def save_k8s_handle(k8s_handle: kubernetes.KubernetesHandle) -> None:
      for job in k8s_handle.jobs:
        namespace = job.metadata.namespace or 'default'
        name = job.metadata.name
        database.database().insert_kubernetes_job(self.experiment_id,
                                                  self.work_unit_id,
                                                  self.work_unit_name,
                                                  namespace, name)

    def throw_on_unknown_handle(handle: Any) -> None:
      raise TypeError(f'Unsupported handle: {handle}')

    for handle in handles:
      pattern_matching.match(save_caip_handle, save_k8s_handle,
                             throw_on_unknown_handle)(
                                 handle)

  async def _wait_until_complete(self) -> None:
    try:
      await asyncio.gather(*[
          handle.wait() for handle in self._local_execution_handles +
          self._non_local_execution_handles
      ])
    except RuntimeError as error:
      raise xm.WorkUnitFailedError(error)

  async def wait_for_local_jobs(self, is_exit_abrupt: bool):
    if not is_exit_abrupt:
      await asyncio.gather(
          *[handle.wait() for handle in self._local_execution_handles])

  def stop(self) -> None:
    """Initiate the process to stop the work unit from running.

    This method will synchronously make a request for the work unit to stop.
    However, the method does not actually wait for the work unit to be in a
    terminal state.

    Use self.wait_until_complete() after self.stop() to guarantee the work unit
    is stopped.
    """
    raise NotImplementedError

  def get_status(self) -> local_status.LocalWorkUnitStatus:
    """Gets the current status of the work unit."""
    handles = self._non_local_execution_handles + self._local_execution_handles
    if len(handles) == 1:
      return handles[0].get_status()
    raise NotImplementedError(
        'Status aggregation for work units with multiple jobs is not '
        'implemented yet.')


class LocalExperiment(xm.Experiment):
  """Experiment contains a family of jobs that run with the local scheduler."""

  _id: int
  _experiment_title: str
  _work_units: List[LocalWorkUnit]

  def __init__(self, experiment_title: str) -> None:
    super().__init__()
    # To distinguish local job names until we have a local database.
    self._id = int(time.time() * 10**3)
    self._experiment_title = experiment_title
    self._work_units = []

  @classmethod
  def package(
      cls, packageables: Sequence[xm.Packageable]) -> Sequence[xm.Executable]:
    """Packages executable specs into executables based on the executor specs."""
    return packaging_router.package(packageables)

  def _create_work_unit(self, args: Mapping[str, Any]) -> LocalWorkUnit:
    work_unit = LocalWorkUnit(self, self._experiment_title,
                              self._work_unit_id_predictor, self._create_task,
                              args)
    database.database().insert_work_unit(self.experiment_id,
                                         work_unit.work_unit_id)
    self._work_units.append(work_unit)
    return work_unit

  def _wait_for_local_jobs(self, is_exit_abrupt: bool):
    if self._work_units:
      print('Waiting for local jobs to complete.'
            ' Press Ctrl+C to terminate them and exit')
    for work_unit in self._work_units:
      self._create_task(work_unit.wait_for_local_jobs(is_exit_abrupt))

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
    return len(self._work_units)

  @property
  def work_units(self) -> Mapping[int, LocalWorkUnit]:
    """Gets work units created via self.add()."""
    raise NotImplementedError


def create_experiment(experiment_title: str) -> xm.Experiment:
  """Create Experiment."""
  experiment = LocalExperiment(experiment_title)
  database.database().insert_experiment(experiment.experiment_id,
                                        experiment._experiment_title)  # pylint: disable=protected-access
  return experiment


def get_experiment(experiment_id: int) -> xm.Experiment:
  """Returns a Experiment instance associated with this experiment id."""
  raise NotImplementedError


def list_experiments() -> Sequence[xm.Experiment]:
  """Yields a list of Experiment instances that have been created thus far."""
  raise NotImplementedError
