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
"""Abstract API Spec for XManager.

Each implementation of the XManager API should override the abstract and
unimplemented methods. Users can then use the implemention via:
from xmanager import impl as xm
"""

import abc
import asyncio
from concurrent import futures
import copy
import functools
import inspect
import threading
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Union

import attr
import immutabledict

from xmanager.xm import id_predictor
from xmanager.xm import pattern_matching

ArgsType = Union[List, Dict]


class ExecutableSpec(abc.ABC):
  """Executable specification describes what code / computation to run.

  An executable spec must turned into an executable using package() in order
  to be used in a Job.
  """


class Executable(abc.ABC):
  """Executable describes the final location of a packaged executable spec.

  An executable depends on the executable specification and the executor
  specification. Experiment's implementation knows how to handle each type of
  executable.
  """


class ExecutorSpec(abc.ABC):
  """Executor spec describes the location of the runtime environment."""


class Executor(abc.ABC):
  """Executor describes the runtime environment of a Job."""

  @classmethod
  @abc.abstractmethod
  def Spec(cls) -> ExecutorSpec:  # pylint: disable=invalid-name
    raise NotImplementedError


@attr.s(auto_attribs=True)
class Packageable:
  """Packageable describes what to build and its static parameters."""

  executable_spec: ExecutableSpec
  executor_spec: ExecutorSpec
  args: ArgsType = attr.Factory(list)
  env_vars: Dict[str, str] = attr.Factory(dict)


class Constraint(abc.ABC):
  """Constraint describes the requirements for where a job group can run.

  Some examples of constraints include:
   * same virtual machine
   * same virtual private cloud subnetwork
   * same network fabric
   * same geographic location
  """


JobGeneratorType = Callable[['WorkUnit'], Awaitable]
JobType = Union['Job', 'JobGroup', JobGeneratorType]


@attr.s(auto_attribs=True)
class Job:
  """Job describes a unique unit of computation that is run only once.

  Jobs have unique identities, so they can only be run once. To launch an
  identical job, a new job should be constructed instead with the same
  arguments.
  """

  executable: Executable
  executor: Executor
  args: ArgsType = attr.Factory(list)
  env_vars: Dict[str, str] = attr.Factory(dict)


class JobGroup:
  """JobGroup describes a set of jobs that run under shared constraints.

  Use named arguments to give jobs meaningful names:
    JobGroup(
      learner=Job(learner_executable, executor)
      actor=Job(actor_executable, executor))

  JobGroups provide the gang scheduling concept: Jobs inside them would be
  scheduled/descheduled simultaneously. Note that scheduler may not always be
  able to enforce that.

  JobGroups may include more fine grained constraints:
    JobGroup(
      learner=Job(tpu_learner_executable, executor)
      preprocessor=Job(preprocessor_executable, executor),
      constraints=[xm.SameMachine()])

  To express sophisticated requirements JobGroups can be nested:
    JobGroup(
      eval=Job(eval_executable, executor)
      colocated_learner_and_actor=JobGroup(
        learner=Job(tpu_learner_executable, executor)
        actor=Job(actor_executable, executor),
        constraints=[xm.SameMachine()]))

  Attributes:
    jobs: Job name to job mapping.
    constraints: A list of additional scheduling constraints.
  """

  jobs: Dict[str, JobType]
  constraints: List[Constraint]

  def __init__(self,
               *,
               constraints: Optional[Iterable[Constraint]] = None,
               **jobs: JobType) -> None:
    """Builds a JobGroup.

    Args:
      constraints: List of additional scheduling constraints. Keyword only arg.
      **jobs: Jobs / job groups that constitute the group passed as kwargs.
    """
    self.jobs = jobs
    self.constraints = list(constraints) if constraints else []


def merge_args(left: ArgsType, right: ArgsType) -> ArgsType:

  def raise_mismatching_types_error(l: Any, r: Any):
    raise ValueError(f'Args types do not match. Must both be dicts or lists, '
                     f'but got {l!r} and {r!r}')

  # pyformat: disable
  return pattern_matching.match(
      pattern_matching.Case([List, List], lambda l, r: [*l, *r]),
      pattern_matching.Case([Dict, Dict], lambda l, r: {**l, **r}),
      raise_mismatching_types_error,
  )(left, right)
  # pyformat: enable


def _apply_args_to_job(job: Job, args: Mapping[str, Any]):
  """Overrides job properties."""
  if args:
    job = copy.copy(job)
    if 'args' in args:
      job.args = merge_args(job.args, args['args'])
    job.env_vars.update(args.get('env_vars', {}))
  return job


def _apply_args_to_job_group(job_group: JobGroup, args: Mapping[str, Any]):
  """Recursively overrides job group properties."""
  if args:
    job_group = copy.copy(job_group)

    job_group.jobs = {
        job_name: _apply_args(job, args.get(job_name, {}))
        for job_name, job in job_group.jobs.items()
    }

  return job_group


_apply_args = pattern_matching.match(
    _apply_args_to_job, _apply_args_to_job_group,
    pattern_matching.Case([JobGeneratorType, Any], lambda other, args: other))


class WorkUnitError(RuntimeError):
  """Work unit could not be completed."""


class WorkUnitFailedError(WorkUnitError):
  """A job running in a work unit has failed."""


class WorkUnitNotCompletedError(WorkUnitError):
  """Work unit is neither running nor completed.

  For example it may be stopped by a user.
  """


def _work_unit_arguments(
    job: JobType,
    args: Mapping[str, Any],
) -> Mapping[str, Any]:
  """Returns work unit arguments.

  Users may choose not to pass `args` to .add method and instead configure
  command line arguments in the xm.Job directly. In this case we deduce work
  unit arguments from the job.

  Args:
    job: The job to run inside a work unit.
    args: Explicetly specified arguments (could be empty).

  Returns:
    Work unit arguments.
  """
  if args:
    # In order to give users control on what is shown as work unit arguments we
    # don't alter them if a value is given.
    return args

  def deduce_args_for_job(job: Job) -> Dict[str, Any]:
    args = {'args': job.args, 'env_vars': job.env_vars}
    return {key: value for key, value in args.items() if value}

  def deduce_args_for_job_group(group: JobGroup) -> Dict[str, Any]:
    return {job_name: deduce_args(job) for job_name, job in group.jobs.items()}

  deduce_args = pattern_matching.match(
      deduce_args_for_job, deduce_args_for_job_group,
      pattern_matching.Case([JobGeneratorType], lambda generator: {}))

  return deduce_args(job)


class WorkUnit(abc.ABC):
  """WorkUnit is a run of experiment Jobs with specific hyperparameters."""

  def __init__(self, experiment: 'Experiment',
               work_unit_id_predictor: id_predictor.Predictor,
               create_task: Callable[[Awaitable[Any]], futures.Future]) -> None:
    """Initializes Worknit instance.

    Args:
      experiment: An experiment this work unit belongs to.
      work_unit_id_predictor: The experiment's ID predictor.
      create_task: A callback to register a new asynchronous task.
    """
    self._work_unit_id_predictor = work_unit_id_predictor
    self.work_unit_id = self._work_unit_id_predictor.reserve_id()
    self._experiment = experiment
    self._create_task = create_task

    self._launched_error = None

  @property
  @functools.lru_cache()
  def _launched_event(self) -> asyncio.Event:
    """Event to be set when the work unit is launched."""
    # In Python < 3.8 `Event` must be created within the event loop. We defer
    # the initialization because the constructor is not run in the loop. Note
    # that after migration to Python >= 3.8 this code can be moved to the
    # constructor.
    return asyncio.Event()

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    return self._experiment.experiment_id

  def add(
      self,
      job: JobType,
      args: Mapping[str, Any] = immutabledict.immutabledict()
  ) -> Awaitable[None]:
    """Adds a Job/JobGroup to the work unit.

    Only one JobGroup can be added to a WorkUnit. This limitation may be lifted
    in the future versions.

    Args:
      job: Job to add.
      args: Keyword arguments to be passed to the job. For Job and JobGroup
        args are recursively expanded. For example:
          wu.add(JobGroup(agent=Job(...)),
                 args={'agent': {'args': {'learning_rate': 0.1}}})
        would update `args` field of a job `agent` in the group.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    job = _apply_args(job, args)

    def launch_job(job: Job) -> Awaitable[None]:
      return self._launch_job_group(
          JobGroup(job=job), _work_unit_arguments(job, args))

    def launch_job_group(group: JobGroup) -> Awaitable[None]:
      return self._launch_job_group(group, _work_unit_arguments(group, args))

    def launch_job_generator(
        job_generator: JobGeneratorType) -> Awaitable[None]:
      if not inspect.iscoroutinefunction(job_generator):
        raise ValueError(
            'Job generator must be an async function. Signature needs to be '
            '`async def job_generator(work_unit: xm.WorkUnit):`')
      return job_generator(self, **args)

    job_awaitable = pattern_matching.match(launch_job, launch_job_group,
                                           launch_job_generator)(
                                               job)

    async def launch():
      try:
        await job_awaitable
      except Exception as e:
        self._launched_error = WorkUnitError(e)
        raise
      finally:
        # Note that the current implementation reuses `_launched_event`: imagine
        # a `work_unit.add(generator)` call -- the generator makes a
        # `work_unit.add` call internally as well, touching `_launched_event`
        # twice in total. Same would apply for multiple external `work_unit.add`
        # calls, which is not allowed yet. `_launched_error` is susceptible to
        # that too.
        self._launched_event.set()

    return asyncio.wrap_future(self._create_task(launch()))

  async def wait_until_complete(self):
    """Waits until work unit is in a final state: completed/failed/stopped.

    Raises:
      WorkUnitError: exception if work unit couldn't complete.
    """
    await self._launched_event.wait()
    if self._launched_error:
      raise self._launched_error
    await self._wait_until_complete()

  async def _launch_job_group(self, job_group: JobGroup,
                              args: Mapping[str, Any]) -> None:
    """Launches a given job group as part of the work unit."""
    raise NotImplementedError

  async def _wait_until_complete(self) -> None:
    """Waits until work unit is in a final state: completed/failed/stopped.

    Child classes need to implement this method to support awaiting work units.

    Unlike wait_until_complete this method asumes that work unit has been fully
    created. This method is only invoked if somebody has requested to monitor
    work unit.
    """
    raise NotImplementedError

  @property
  def job_count(self) -> int:
    raise NotImplementedError


class Experiment(abc.ABC):
  """Experiment contains a family of jobs run on the same snapshot of code.

  Experiment also implements the behavior of how to add and execute jobs.
  Attempting to add jobs that contain Executables with unsupported types will
  fail.
  """

  # An event loop in which job generators would be run.
  _event_loop: asyncio.AbstractEventLoop
  # A list of background tasks that launch work units.
  _running_tasks: List[futures.Future]
  # Work unit ID predictor.
  _work_unit_id_predictor: id_predictor.Predictor

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    raise NotImplementedError

  def _enter(self) -> None:
    """Initializes internal state on context manager enter."""
    self._running_tasks = []
    self._work_unit_id_predictor = id_predictor.Predictor(1 +
                                                          self.work_unit_count)

  def __enter__(self):
    if asyncio.get_event_loop().is_running():
      raise RuntimeError('When using Experiment from a coroutine plase use '
                         '`async with` syntax')

    self._event_loop = asyncio.new_event_loop()
    asyncio.get_child_watcher().attach_loop(self._event_loop)
    self._event_loop_thread = threading.Thread(
        target=self._event_loop.run_forever, daemon=True)
    self._event_loop_thread.start()

    self._enter()
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    for task in self._running_tasks:
      task.result()

    self._event_loop.call_soon_threadsafe(self._event_loop.stop)
    self._event_loop_thread.join()

  async def __aenter__(self):
    self._event_loop = asyncio.get_event_loop()
    self._enter()
    return self

  async def __aexit__(self, exc_type, exc_value, traceback):
    for task in self._running_tasks:
      await asyncio.wrap_future(task)

  @abc.abstractmethod
  def package(self,
              packageables: Iterable[Packageable]) -> Iterable[Executable]:
    """Packages executable specs into executables based on the executor specs."""
    raise NotImplementedError

  def add(
      self,
      job: JobType,
      args: Mapping[str, Any] = immutabledict.immutabledict()
  ) -> Awaitable[WorkUnit]:
    """Adds a Job/JobGroup to the experiment.

    A new WorkUnit is created to run the job.

    Args:
      job: Job to add.
      args: Keyword arguments to be passed to the job. For Job and JobGroup
        args are recursively expanded. For example:
          wu.add(JobGroup(agent=Job(...)),
                 args={'agent': {'args': {'learning_rate': 0.1}}})
        would update `args` field of a job `agent` in the group.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    work_unit = self._create_work_unit()

    async def launch():
      await work_unit.add(job, args)
      return work_unit

    return asyncio.wrap_future(self._create_task(launch()))

  def _create_work_unit(self) -> WorkUnit:
    """Creates a new WorkUnit instance for the experiment."""
    return WorkUnit(self, self._work_unit_id_predictor, self._create_task)

  def _create_task(self, task: Awaitable[Any]) -> futures.Future:
    future = asyncio.run_coroutine_threadsafe(task, loop=self._event_loop)
    self._running_tasks.append(future)
    return future

  @property
  def work_unit_count(self) -> int:
    """Returns how many work units the experiment has."""
    raise NotImplementedError


# FIXME: Determine base Experiment properties.
@abc.abstractmethod
def create_experiment() -> Experiment:
  """Returns a concrete Experiment instance."""
  raise NotImplementedError
