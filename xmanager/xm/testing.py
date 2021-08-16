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
"""Utilities for testing core objects."""

from concurrent import futures
from typing import Any, Awaitable, Callable, Iterable, List, Mapping

from xmanager.xm import core
from xmanager.xm import id_predictor
from xmanager.xm import job_blocks


class TestWorkUnit(core.WorkUnit):
  """A test version of WorkUnit with abstract methods implemented."""

  def __init__(
      self,
      experiment: core.Experiment,
      work_unit_id_predictor: id_predictor.Predictor,
      create_task: Callable[[Awaitable[Any]], futures.Future],
      launched_jobs: List[job_blocks.JobType],
      launched_jobs_args: List[Mapping[str, Any]],
      args: Mapping[str, Any],
  ) -> None:
    super().__init__(experiment, work_unit_id_predictor, create_task, args)
    self._launched_jobs = launched_jobs
    self._launched_jobs_args = launched_jobs_args

  async def _wait_until_complete(self) -> None:
    """Test work unit is immediately complete."""

  async def _launch_job_group(self, job_group: job_blocks.JobGroup,
                              args: Mapping[str, Any]) -> None:
    """Appends the job group to the launched_jobs list."""
    self._launched_jobs.extend(job_group.jobs.values())
    self._launched_jobs_args.append(args)


class TestExperiment(core.Experiment):
  """A test version of Experiment with abstract methods implemented."""

  constraints: List[job_blocks.JobType]

  def __init__(self) -> None:
    super().__init__()
    self.launched_jobs = []
    self.launched_jobs_args = []
    self._work_units = []

  def package(
      self, packageables: Iterable[job_blocks.Packageable]
  ) -> Iterable[job_blocks.Executable]:
    """Packages executable specs into executables based on the executor specs."""
    raise NotImplementedError

  def _create_work_unit(self, args: Mapping[str, Any]) -> TestWorkUnit:
    """Creates a new WorkUnit instance for the experiment."""
    work_unit = TestWorkUnit(self, self._work_unit_id_predictor,
                             self._create_task, self.launched_jobs,
                             self.launched_jobs_args, args)
    self._work_units.append(work_unit)
    return work_unit

  @property
  def work_unit_count(self) -> int:
    return len(self.work_units)

  @property
  def work_units(self):
    return self._work_units

  @property
  def experiment_id(self) -> int:
    return 1


class TestExecutable(job_blocks.Executable):
  """A test version of Executable with abstract methods implemented."""
  counter = 0

  def __init__(self):
    super().__init__(name=f'{TestExecutable.counter}')
    TestExecutable.counter += 1


class TestExecutor(job_blocks.Executor):
  """A test version of Executor with abstract methods implemented."""

  Spec = job_blocks.ExecutorSpec  # pylint: disable=invalid-name
