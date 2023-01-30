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
"""A mock implemenation of core XM objects.

This module is mainly purposed for unit tests. It provides implementation of
xm.Experiment, xm.WorkUnit, xm.MetadataContext which don't do much apart from
allowing to inspect what methods were called.
It also may be used as a stub implementation for cases when one doesn't care
how experiment is set up, but wants to maintain API consistency. For examle if
a framework provides own local run mode and wants to ignore any experiment
metadata manipulations.
"""

import asyncio
from concurrent import futures
from typing import Any, Awaitable, Callable, List, Mapping, Optional, Set

import attr
from xmanager.xm import async_packager
from xmanager.xm import core
from xmanager.xm import id_predictor
from xmanager.xm import job_blocks
from xmanager.xm import metadata_context
from xmanager.xm import pattern_matching as pm


class MockContextAnnotations(metadata_context.ContextAnnotations):
  """ContextAnnotations which stores all data in memory."""

  def __init__(self) -> None:
    self._title = ''
    self._tags = set()
    self._notes = ''

  @property
  def title(self) -> str:
    return self._title

  def set_title(self, title: str) -> None:
    self._title = title

  @property
  def tags(self) -> Set[str]:
    return self._tags

  def add_tags(self, *tags: str) -> None:
    self._tags.update(tags)

  def remove_tags(self, *tags: str) -> None:
    for tag in tags:
      self._tags.discard(tag)

  @property
  def notes(self) -> str:
    return self._notes

  def set_notes(self, notes: str) -> None:
    self._notes = notes


class MockMetadataContext(metadata_context.MetadataContext):
  """A MetadataContext which stores all data in memory."""

  def __init__(self) -> None:
    super().__init__(creator='unknown', annotations=MockContextAnnotations())


class MockExperimentUnit(core.WorkUnit):
  """A mock version of WorkUnit with abstract methods implemented."""

  def __init__(
      self,
      experiment: core.Experiment,
      work_unit_id_predictor: id_predictor.Predictor,
      create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
      launched_jobs: List[job_blocks.JobType],
      launched_jobs_args: List[Optional[Mapping[str, Any]]],
      args: Optional[Mapping[str, Any]],
      role: core.ExperimentUnitRole,
  ) -> None:
    super().__init__(experiment, create_task, args, role)
    self._launched_jobs = launched_jobs
    self._launched_jobs_args = launched_jobs_args
    self._work_unit_id = work_unit_id_predictor.reserve_id()

  async def _wait_until_complete(self) -> None:
    """Mock work unit is immediately complete."""

  async def _launch_job_group(
      self,
      job_group: job_blocks.JobGroup,
      args: Optional[Mapping[str, Any]],
      identity: str,
  ) -> None:
    """Appends the job group to the launched_jobs list."""
    self._launched_jobs.extend(job_group.jobs.values())
    self._launched_jobs_args.append(args)

  @property
  def work_unit_id(self) -> int:
    return self._work_unit_id

  @property
  def experiment_unit_name(self) -> str:
    return f'{self.experiment_id}_{self._work_unit_id}'


class MockExperiment(core.Experiment):
  """A mock version of Experiment with abstract methods implemented."""

  constraints: List[job_blocks.JobType]

  _async_packager = async_packager.AsyncPackager(lambda _: [])

  def __init__(self) -> None:
    super().__init__()
    self.launched_jobs = []
    self.launched_jobs_args = []
    self._work_units = []
    self._auxiliary_units = []

    self._context = MockMetadataContext()

  def _create_experiment_unit(
      self,
      args: Optional[Mapping[str, Any]],
      role: core.ExperimentUnitRole = core.WorkUnitRole(),
      identity: str = '',
  ) -> Awaitable[MockExperimentUnit]:
    """Creates a new WorkUnit instance for the experiment."""
    del identity  # Unused.
    future = asyncio.Future(loop=self._event_loop)
    experiment_unit = MockExperimentUnit(
        self,
        self._work_unit_id_predictor,
        self._create_task,
        self.launched_jobs,
        self.launched_jobs_args,
        args,
        role,
    )
    pm.match(
        pm.Case(
            [core.WorkUnitRole],
            lambda _: self._work_units.append(experiment_unit),
        ),
        pm.Case(
            [core.AuxiliaryUnitRole],
            lambda _: self._auxiliary_units.append(experiment_unit),
        ),
    )(role)

    future.set_result(experiment_unit)
    return future

  @property
  def work_unit_count(self) -> int:
    return len(self.work_units)

  @property
  def work_units(self):
    return self._work_units

  @property
  def auxiliary_units(self):
    return self._auxiliary_units

  @property
  def experiment_id(self) -> int:
    return 1

  @property
  def context(self) -> MockMetadataContext:
    """Returns metadata context for the experiment."""
    return self._context


class MockExecutable(job_blocks.Executable):
  """A mock version of Executable with abstract methods implemented."""

  counter = 0

  def __init__(self):
    super().__init__(name=f'{MockExecutable.counter}')
    MockExecutable.counter += 1


class MockExecutor(job_blocks.Executor):
  """A mock version of Executor with abstract methods implemented."""

  Spec = job_blocks.ExecutorSpec  # pylint: disable=invalid-name


@attr.s(auto_attribs=True)
class MockConstraint(job_blocks.Constraint):
  id: str
