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
unimplemented methods. Users can then use the implementions via:

```
from xmanager import xm
from xmanager import xm_foo
```
"""

import abc
import asyncio
from concurrent import futures
import copy
import functools
import inspect
import queue
import threading
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Union

import attr
import immutabledict

from xmanager.xm import id_predictor
from xmanager.xm import metadata_context
from xmanager.xm import pattern_matching

ArgsType = Union[List, Dict]


class ExecutableSpec(abc.ABC):
  """Executable specification describes what code / computation to run.

  An executable spec must turned into an executable using package() in order
  to be used in a Job.
  """

  @property
  @abc.abstractmethod
  def name(self) -> str:
    raise NotImplementedError


@attr.s(auto_attribs=True)
class Executable(abc.ABC):
  """Executable describes the final location of a packaged executable spec.

  An executable depends on the executable specification and the executor
  specification. Experiment's implementation knows how to handle each type of
  executable.

  Attributes:
      name: An automatically populated name for the executable. Used for
          assigning default names to `Job`s.
  """

  name: str


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
  """Job describes a unit of computation to be run.

  Attributes:
    executable: What to run -- one of `xm.Experiment.package` results.
    executor: Where to run -- one of `xm.Executor` subclasses.
    name: Name of the job. Must be unique within the context (work unit). By
      default it is constructed from the executable. Used for naming related
      entities such as newly created containers.
    args: Command line arguments to pass.
    env_vars: Environment variables to apply.
  """

  executable: Executable
  executor: Executor
  name: Optional[str] = None
  args: ArgsType = attr.Factory(list)
  env_vars: Dict[str, str] = attr.Factory(dict)


class JobGroup:
  """JobGroup describes a set of jobs that run under shared constraints.

  Use named arguments to give jobs meaningful names:

  ```
  JobGroup(
      learner=Job(learner_executable, executor),
      actor=Job(actor_executable, executor),
  )
  ```

  JobGroups provide the gang scheduling concept: Jobs inside them would be
  scheduled / descheduled simultaneously. Note that schedulers may not always be
  able to enforce that.

  JobGroups may include more fine grained constraints:

  ```
  JobGroup(
      learner=Job(tpu_learner_executable, executor),
      preprocessor=Job(preprocessor_executable, executor),
      constraints=[xm.SameMachine()],
  )
  ```

  To express sophisticated requirements JobGroups can be nested:

  ```
  JobGroup(
      eval=Job(eval_executable, executor),
      colocated_learner_and_actor=JobGroup(
          learner=Job(tpu_learner_executable, executor),
          actor=Job(actor_executable, executor),
          constraints=[xm.SameMachine()],
      ),
  )
  ```

  Attributes:
    jobs: A mapping of names to jobs.
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
  """Merges two argument sets."""
  if not left:
    return right
  if not right:
    return left

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
        key: _apply_args(job, args.get(key, {}))
        for key, job in job_group.jobs.items()
    }

  return job_group


_apply_args = pattern_matching.match(
    _apply_args_to_job, _apply_args_to_job_group,
    pattern_matching.Case([JobGeneratorType, Any], lambda other, args: other))


class WorkUnitStatus(abc.ABC):
  """The status of a work unit."""

  @abc.abstractmethod
  def is_running(self) -> bool:
    raise NotImplementedError

  @abc.abstractmethod
  def is_succeeded(self) -> bool:
    raise NotImplementedError

  @abc.abstractmethod
  def is_failed(self) -> bool:
    raise NotImplementedError

  @abc.abstractmethod
  def message(self) -> str:
    """An optional human-readable message providing context for the status.

    This may take the form of explaining why the work unit is in this state,
    or any potentially transient errors the work unit may be experiencing.
    """
    raise NotImplementedError


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
  """Constructs work unit arguments to display them in various UIs.

  If users pass `args` to the `.add` method explicitly, we assume `args` to be
  the sought work unit arguments. If `args` are not passed to `.add`, we deduce
  work unit arguments implicitly from the `job`s' `args` and `env_vars`.

  Args:
    job: A job to run inside a work unit.
    args: Explicitly specified arguments (could be empty).

  Returns:
    Depending on the type of the `job` given, can be one of the following:

      - if it's an instance of `Job`, we return `{'args': job.args, 'env_vars':
        job.env_vars}` with empty values omitted;
      - if it's an instance of `JobGroup`, we recursively unwind the group while
        populating corresponding nested dictionaries until we reach standalone
        `Job`s;
      - if it's a job generator, we return `{}`.
  """
  if args:
    # In order to give users control on what is shown as work unit arguments we
    # don't alter them if a value is given.
    return args

  def deduce_args_for_job(job: Job) -> Dict[str, Any]:
    args = {'args': job.args, 'env_vars': job.env_vars}
    return {key: value for key, value in args.items() if value}

  def deduce_args_for_job_group(group: JobGroup) -> Dict[str, Any]:
    args = {}
    for job_name, job in group.jobs.items():
      job_args = deduce_args(job)
      if job_args:
        args[job_name] = job_args
    return args

  deduce_args = pattern_matching.match(
      deduce_args_for_job, deduce_args_for_job_group,
      pattern_matching.Case([JobGeneratorType], lambda generator: {}))

  return deduce_args(job)


def _populate_job_names(job: JobType) -> JobType:
  """Assigns default names to the given job(s).

  Args:
      job: A `Job` / `JobGroup` to populate names for.

  Returns:
      Job(s) with `name`(s) populated (if they were absent).
  """

  def apply_to_job(target: Job):
    if target.name is None:
      target = copy.copy(target)
      target.name = target.executable.name
    return target

  def apply_to_job_group(target: JobGroup):
    target = copy.copy(target)
    target.jobs = {key: matcher(job) for key, job in target.jobs.items()}
    return target

  def ignore_unknown(target: Any):
    return target

  matcher = pattern_matching.match(
      apply_to_job,
      apply_to_job_group,
      ignore_unknown,
  )
  return matcher(job)


class WorkUnit(abc.ABC):
  """WorkUnit is a collection of semantically associated `Job`s."""

  def __init__(self, experiment: 'Experiment',
               work_unit_id_predictor: id_predictor.Predictor,
               create_task: Callable[[Awaitable[Any]], futures.Future]) -> None:
    """Initializes a `WorkUnit` instance.

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
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    return self._experiment.experiment_id

  @property
  @functools.lru_cache()
  def _launched_event(self) -> asyncio.Event:
    """Event to be set when the work unit is launched."""
    # In Python < 3.8 `Event` must be created within the event loop. We defer
    # the initialization because the constructor is not run in the loop. Note
    # that after migration to Python >= 3.8 this code can be moved to the
    # constructor.
    return asyncio.Event()

  def add(
      self,
      job: JobType,
      args: Mapping[str, Any] = immutabledict.immutabledict()
  ) -> Awaitable[None]:
    # pyformat: disable
    """Adds a Job / JobGroup to the work unit.

    Only one JobGroup can be added to a WorkUnit. This limitation may be lifted
    in future versions.

    Args:
      job: A job or job group to add.
      args: Keyword arguments to be passed to the job. For Job and JobGroup args
        are recursively expanded. For example,

        ```
        wu.add(
            JobGroup(agent=Job(...)),
            args={'agent': {'args': {'learning_rate': 0.1}}},
        )
        ```

        would update `args` field of a job `agent` in the group.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    # pyformat: enable
    job = _apply_args(job, args)
    job = _populate_job_names(job)

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
    """Waits until the work unit is in a final state: completed/failed/stopped.

    Raises:
      WorkUnitError: Exception if the work unit couldn't complete.
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
    """Waits until the work unit is in a final state: completed/failed/stopped.

    Child classes need to implement this method to support awaiting work units.

    Unlike wait_until_complete this method asumes that work unit has been fully
    created. This method is only invoked if somebody has requested to monitor
    work unit.
    """
    raise NotImplementedError

  def stop(self) -> None:
    """Initiate the process to stop the work unit from running.

    This method will synchronously make a request for the work unit to stop.
    However, the method does not actually wait for the work unit to be in a
    terminal state.

    Use self.wait_until_complete() after self.stop() to guarantee the work unit
    is stopped.
    """
    raise NotImplementedError

  def get_status(self) -> WorkUnitStatus:
    """Gets the status of this work unit."""
    raise NotImplementedError

  @property
  def work_unit_name(self) -> str:
    return f'{self.experiment_id}_{self.work_unit_id}'

  def get_full_job_name(self, job_name: str) -> str:
    """Given `Job.name` constructs its full name.

    The primary use case is addressing containers -- full names serve as
    hostnames.

    Args:
        job_name: Short name of a job.

    Returns:
        Full name of the job.
    """
    return f'{self.work_unit_name}_{job_name}'

  @property
  def context(self) -> metadata_context.MetadataContext:
    """Returns metadata context for a work unit."""
    return metadata_context.MetadataContext(
        annotations=metadata_context.ContextAnnotations())


class Experiment(abc.ABC):
  """Experiment contains a family of jobs run on the same snapshot of code.

  Experiment also implements the behavior of how to add and execute jobs.
  Attempting to add jobs that contain Executables with unsupported types will
  fail.
  """

  # An event loop in which job generators would be run.
  _event_loop: asyncio.AbstractEventLoop
  # A queue of background tasks that launch work units.
  _running_tasks: queue.Queue
  # Work unit ID predictor.
  _work_unit_id_predictor: id_predictor.Predictor

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    raise NotImplementedError

  def _enter(self) -> None:
    """Initializes internal state on context manager enter."""
    self._running_tasks = queue.Queue()
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

  def _wait_for_tasks(self):
    while not self._running_tasks.empty():
      self._running_tasks.get_nowait().result()

  def __exit__(self, exc_type, exc_value, traceback):
    self._wait_for_tasks()
    self._event_loop.call_soon_threadsafe(self._event_loop.stop)
    self._event_loop_thread.join()

  async def __aenter__(self):
    self._event_loop = asyncio.get_event_loop()
    self._enter()
    return self

  async def _await_for_tasks(self):
    while not self._running_tasks.empty():
      await asyncio.wrap_future(self._running_tasks.get_nowait())

  async def __aexit__(self, exc_type, exc_value, traceback):
    await self._await_for_tasks()

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
    # pyformat: disable
    """Adds a Job / JobGroup to the experiment.

    A new WorkUnit is created to run the job.

    Args:
      job: A Job or JobGroup to add.
      args: Keyword arguments to be passed to the job. For Job and JobGroup args
        are recursively expanded. For example,

        ```
        wu.add(
            JobGroup(agent=Job(...)),
            args={'agent': {'args': {'learning_rate': 0.1}}},
        )
        ```

        would update `args` field of a job `agent` in the group.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    # pyformat: enable
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
    self._running_tasks.put_nowait(future)
    return future

  @property
  def work_unit_count(self) -> int:
    """Returns how many work units the experiment has."""
    raise NotImplementedError

  @abc.abstractmethod
  def work_units(self) -> Mapping[int, WorkUnit]:
    """Returns a mapping from work_unit_id to an instance of the work unit."""
    raise NotImplementedError

  @property
  def context(self) -> metadata_context.MetadataContext:
    """Returns metadata context for a work unit."""
    return metadata_context.MetadataContext(
        annotations=metadata_context.ContextAnnotations())


# FIXME: Determine base Experiment properties.
@abc.abstractmethod
def create_experiment() -> Experiment:
  """Returns a concrete Experiment instance."""
  raise NotImplementedError


@abc.abstractmethod
def get_experiment(experiment_id: int) -> Experiment:
  """Returns a Experiment instance associated with this experiment id."""
  raise NotImplementedError
