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
"""Abstract API specification for XManager implementations.

Each implementation of the XManager API should override the abstract methods.
Users are normally expected to have the following pair of imports:

```
from xmanager import xm
from xmanager import xm_foo
```
"""

import abc
import asyncio
from concurrent import futures
import enum
import functools
import getpass
import inspect
import queue
import threading
from typing import Any, Awaitable, Callable, Collection, Dict, Mapping, Optional, Sequence, overload

import attr
from xmanager.xm import async_packager
from xmanager.xm import id_predictor
from xmanager.xm import job_blocks
from xmanager.xm import job_operators
from xmanager.xm import metadata_context
from xmanager.xm import pattern_matching


def _check_if_unsupported_args_are_present(args: Mapping[str, Any],
                                           supported_args: Collection[str],
                                           job_type: str) -> None:
  supported_args = set(supported_args)
  unsupported_args = set(args.keys()) - supported_args
  if unsupported_args:
    raise ValueError(
        f'Arguments {unsupported_args!r} are not supported by {job_type}. Only '
        f'{supported_args!r} are allowed.')


def _apply_args_to_job(job: job_blocks.Job, args: Mapping[str, Any]) -> None:
  """Overrides job properties."""
  _check_if_unsupported_args_are_present(args, ('args', 'env_vars'), 'xm.Job')

  if 'args' in args:
    job.args = job_blocks.merge_args(job.args, args['args'])
  job.env_vars.update(args.get('env_vars', {}))


def _apply_args_to_job_group(job_group: job_blocks.JobGroup,
                             args: Mapping[str, Any]) -> None:
  """Recursively overrides job group properties."""
  if args:
    _check_if_unsupported_args_are_present(args, job_group.jobs.keys(),
                                           'xm.JobGroup')
    for key, job in job_group.jobs.items():
      _apply_args(job, args.get(key, {}))


_apply_args = pattern_matching.match(
    _apply_args_to_job, _apply_args_to_job_group,
    pattern_matching.Case([job_blocks.JobGeneratorType, Any],
                          lambda other, args: None))


class ExperimentUnitStatus(abc.ABC):
  """The status of an experiment unit."""

  @property
  @abc.abstractmethod
  def is_active(self) -> bool:
    """Returns whether the unit is not in terminal state.

    It may be actively running or queued. The unit may produce more results.
    If the unit is stopped by a user it will be neither active, completed
    nor failed.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def is_completed(self) -> bool:
    """Returns whether the unit has completed without failures.

    This is a terminal state. The unit has produced all the intended results.
    But it still may be restarted by an explicit request.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def is_failed(self) -> bool:
    """Returns whether the unit has failed.

    This is a terminal state. Experiment unit will enter this state on any
    fatal failure, such as process exiting with non-zero code, cloud rejecting
    to schedule/queue the job or exceptions in JobGenerator. The unit will stay
    in this state unless explicitly restarted.
    Intermediate failures do not result in this state.
    """
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def message(self) -> str:
    """An optional human-readable message providing context for the status.

    This may take the form of explaining why the work unit is in this state,
    or any potentially transient errors the work unit may be experiencing.
    """
    raise NotImplementedError


class ExperimentUnitError(RuntimeError):
  """Experiment unit could not be completed."""


class ExperimentUnitFailedError(ExperimentUnitError):
  """A job running in an experiment unit has failed."""


class ExperimentUnitNotCompletedError(ExperimentUnitError):
  """Experiment unit is neither running nor completed.

  For example it may be stopped by a user.
  """


class NotFoundError(KeyError):
  """Experiment/Work Unit/etc. has not been found."""


def _work_unit_arguments(
    job: job_blocks.JobType,
    args: Optional[Mapping[str, Any]],
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
  if args is not None:
    # In order to give users control on what is shown as work unit arguments we
    # don't alter them if a value is given.
    return args

  def deduce_args_for_job(job: job_blocks.Job) -> Dict[str, Any]:
    args = {
        'args': job.args.to_dict(kwargs_only=True),
        'env_vars': job.env_vars
    }
    return {key: value for key, value in args.items() if value}

  def deduce_args_for_job_group(group: job_blocks.JobGroup) -> Dict[str, Any]:
    args = {}
    for job_name, job in group.jobs.items():
      job_args = deduce_args(job)
      if job_args:
        args[job_name] = job_args
    return args

  deduce_args = pattern_matching.match(
      deduce_args_for_job, deduce_args_for_job_group,
      pattern_matching.Case([job_blocks.JobGeneratorType],
                            lambda generator: {}))

  return deduce_args(job)


class Importance(enum.Enum):
  """How important it is to schedule particular Experiment or ExperimentUnit.

  This is a hint to the scheduler. Not all schedulers take it into account
  (xm_local doesn't). And even with smart scheduler a less important work unit
  may run before a more important one e.g. if it uses a less contended resource.

  Unlike ServiceTier, importance only controls preference within a team i.e. how
  team's resources are divided between team's experiments. It has no effect on
  resource allocation between teams.
  """

  # High impact experiments. Try scheduling them even at the cost of significant
  # reduction of the overall throughput that your experiments get.
  HIGH = 'high'
  # The default importance.
  NORMAL = 'normal'
  # Prefer to schedule other experiments with higher importance, but in overall
  # try to maximize throughput.
  LOW = 'low'


@attr.s(auto_attribs=True, kw_only=True)
class ExperimentUnitRole(abc.ABC):
  """The role of an experiment unit within the experiment structure.

  Attributes:
    importance: how important it is to schedule this executable unit comparing
      to all your executable units (from all your experiments).
  """
  importance: Importance = Importance.NORMAL


class ExperimentUnit(abc.ABC):
  """ExperimentUnit is a collection of semantically associated `Job`s."""

  def __init__(self, experiment: 'Experiment',
               create_task: Callable[[Awaitable[Any]], futures.Future],
               args: Optional[Mapping[str,
                                      Any]], role: ExperimentUnitRole) -> None:
    """Initializes an `ExperimentUnit` instance.

    Args:
      experiment: An experiment this unit belongs to.
      create_task: A callback to register a new asynchronous task.
      args: Arguments to this experiment unit. Most commonly used to represent
        the hyperparameter sweep trial corresponding to a work unit.
      role: The role of this unit in the experiment structure.
    """
    self._experiment = experiment
    self._create_task = create_task
    self._args = args
    self._role = role

    self._launched_error = None

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    return self._experiment.experiment_id

  @property
  @functools.lru_cache()
  def _launched_event(self) -> asyncio.Event:
    """Event to be set when the experiment unit is launched."""
    # In Python < 3.8 `Event` must be created within the event loop. We defer
    # the initialization because the constructor is not run in the loop. Note
    # that after migration to Python >= 3.8 this code can be moved to the
    # constructor.
    return asyncio.Event()

  def add(self,
          job: job_blocks.JobType,
          args: Optional[Mapping[str, Any]] = None) -> Awaitable[None]:
    # pyformat: disable
    """Adds a Job / JobGroup to the experiment unit.

    Only one JobGroup can be added to an ExperimentUnit. This limitation may be
    lifted in future versions.

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
    job = job_operators.shallow_copy_job_type(job)
    if args is not None:
      _apply_args(job, args)
    job_operators.populate_job_names(job)

    def launch_job(job: job_blocks.Job) -> Awaitable[None]:
      return self._launch_job_group(
          job_blocks.JobGroup(**{job.name: job}),
          _work_unit_arguments(job, self._args))

    def launch_job_group(group: job_blocks.JobGroup) -> Awaitable[None]:
      return self._launch_job_group(group,
                                    _work_unit_arguments(group, self._args))

    def launch_job_generator(
        job_generator: job_blocks.JobGeneratorType) -> Awaitable[None]:
      if not inspect.iscoroutinefunction(job_generator):
        raise ValueError(
            'Job generator must be an async function. Signature needs to be '
            '`async def job_generator(work_unit: xm.WorkUnit):`')
      return job_generator(self, **(args or {}))

    job_awaitable = pattern_matching.match(launch_job, launch_job_group,
                                           launch_job_generator)(
                                               job)

    async def launch():
      try:
        await job_awaitable
      except Exception as e:
        self._launched_error = ExperimentUnitError(e)
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

  async def wait_until_complete(self) -> 'ExperimentUnit':
    """Waits until the unit is in a final state: completed/failed/stopped.

    Raises:
      ExperimentUnitError: Exception if the unit couldn't complete.
    Returns:
      Returns self to facilitate asyncio.as_completed usage.
    """
    await self._launched_event.wait()
    if self._launched_error:
      raise self._launched_error
    await self._wait_until_complete()
    return self

  async def _launch_job_group(self, job_group: job_blocks.JobGroup,
                              args_view: Mapping[str, Any]) -> None:
    """Launches a given job group as part of the unit."""
    raise NotImplementedError

  async def _wait_until_complete(self) -> None:
    """Waits until the unit is in a final state: completed/failed/stopped.

    Child classes need to implement this method to support awaiting units.

    Unlike wait_until_complete this method asumes that unit has been fully
    created. This method is only invoked if somebody has requested to monitor
    unit.
    """
    raise NotImplementedError

  def stop(self) -> None:
    """Initiate the process to stop the unit from running.

    This method will synchronously make a request for the unit to stop.
    However, the method does not actually wait for the unit to be in a
    terminal state.

    Use self.wait_until_complete() after self.stop() to guarantee the unit
    is stopped.
    """
    raise NotImplementedError

  def get_status(self) -> ExperimentUnitStatus:
    """Gets the status of this unit."""
    raise NotImplementedError

  @property
  @abc.abstractmethod
  def experiment_unit_name(self) -> str:
    raise NotImplementedError

  def get_full_job_name(self, job_name: str) -> str:
    """Given `Job.name` constructs its full name.

    The primary use case is addressing containers -- full names serve as
    hostnames.

    Args:
        job_name: Short name of a job.

    Returns:
        Full name of the job.
    """
    return f'{self.experiment_unit_name}_{job_name}'

  @property
  def context(self) -> metadata_context.MetadataContext:
    """Returns metadata context for a unit."""
    return metadata_context.MetadataContext(
        creator=getpass.getuser(),
        annotations=metadata_context.ContextAnnotations())


@attr.s(auto_attribs=True, kw_only=True)
class WorkUnitRole(ExperimentUnitRole):
  """An experiment unit with this role is a work unit.

  Work units contain jobs that are often run as trials as part of an
  experiment's hyper-parameter search. The status of a work unit is used to
  determine the status of the experiment.
  """


class WorkUnit(ExperimentUnit):
  """Work units are experiment units with the work unit role."""

  @property
  @abc.abstractmethod
  def work_unit_id(self) -> int:
    raise NotImplementedError

  async def wait_until_complete(self) -> 'WorkUnit':
    """Waits until the unit is in a final state: completed/failed/stopped.

    Raises:
      ExperimentUnitError: Exception if the unit couldn't complete.
    Returns:
      Returns self to facilitate asyncio.as_completed usage.
    """
    await super().wait_until_complete()
    return self


@attr.s(auto_attribs=True, kw_only=True)
class AuxiliaryUnitRole(ExperimentUnitRole):
  """An experiment unit with this role is an auxiliary unit.

  Auxiliary units contain jobs that are not part of the trials of a
  hyper-parameter search. The status of an auxiliary unit is not used to
  determine the status of the experiment. e.g. Tensorboard

  Attributes:
    termination_delay_secs: How long to keep AUX unit running after experiment
      completion.
  """

  termination_delay_secs: int


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
  # A class variable for batching packaging requests.
  _async_packager: async_packager.AsyncPackager

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    raise NotImplementedError

  def __enter__(self):
    if asyncio.get_event_loop().is_running():
      raise RuntimeError('When using Experiment from a coroutine please use '
                         '`async with` syntax')

    self._event_loop = asyncio.new_event_loop()
    asyncio.get_child_watcher().attach_loop(self._event_loop)
    self._event_loop_thread = threading.Thread(
        target=self._event_loop.run_forever, daemon=True)
    self._event_loop_thread.start()

    # asyncio.run_coroutine_threadsafe doesn't accept class method and wants it
    # wrapped in a function.
    async def async_enter():
      await self.__aenter__()

    asyncio.run_coroutine_threadsafe(
        async_enter(), loop=self._event_loop).result()

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
    self._running_tasks = queue.Queue()
    self._work_unit_id_predictor = id_predictor.Predictor(1 +
                                                          self.work_unit_count)
    return self

  async def _await_for_tasks(self):
    while not self._running_tasks.empty():
      await asyncio.wrap_future(self._running_tasks.get_nowait())

  async def __aexit__(self, exc_type, exc_value, traceback):
    await self._await_for_tasks()

  @classmethod
  def package(
      cls, packageables: Sequence[job_blocks.Packageable] = ()
  ) -> Sequence[job_blocks.Executable]:
    """Packages `packageables` & triggers async packaging.

    This function has 2 usages:
    - Builds all given executables specs in parallel. While calling package(...)
      multiple times is allowed, that would result in slow sequential build,
      even if invoked from concurrent threads.
    - Triggers packaging of the items enqueued previously with `package_async`.

    Args:
      packageables: A sequence of extra packageables to build synchronously.

    Returns:
      A sequence of packaging results associated to `packageables` (same order).
    """
    return cls._async_packager.package(packageables)

  @classmethod
  def package_async(
      cls,
      packageable: job_blocks.Packageable) -> Awaitable[job_blocks.Executable]:
    """Queues executable spec to be packaged into executable.

    If gathering all packageables for a single `package()` call is inconvenient,
    one may request packaging with `package_async` and later trigger the build
    for the whole batch with `package()`.

    Usage:

      if eval:
        eval_executable = experiment.package_async(xm.blaze_binary(...))
      if train:
        train_executable = experiment.package_async(xm.blaze_binary(...))

      experiment.package()  # Explicitly trigger packaging.

      jobs = {}
      if eval:
        jobs['eval'] = xm.job(await eval_executable, ...)
      if train:
        jobs['train'] = xm.job(await train_executable, ...)

    Args:
      packageable: Executable spec to package.

    Returns:
      An awaitable for the packaging result.
    """
    return cls._async_packager.add(packageable)

  @overload
  def add(self,
          job: job_blocks.JobType,
          args: Optional[Mapping[str, Any]] = ...,
          role: WorkUnitRole = ...) -> Awaitable[WorkUnit]:
    ...

  @overload
  def add(self, job: job_blocks.JobType, args: Optional[Mapping[str, Any]],
          role: ExperimentUnitRole) -> Awaitable[ExperimentUnit]:
    ...

  @overload
  def add(
      self,
      job: job_blocks.JobType,
      args: Optional[Mapping[str, Any]] = ...,
      *,  # parameters after “*” are keyword-only parameters
      role: ExperimentUnitRole
  ) -> Awaitable[ExperimentUnit]:
    ...

  # The ExecutableUnit return type is determined by the role.
  def add(self, job, args=None, role=WorkUnitRole()):
    # pyformat: disable
    """Adds a Job / JobGroup to the experiment.

    A new Experiment Unit is created to run the job.

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
      role: The role of this unit in the experiment structure.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    # pyformat: enable
    experiment_unit_future = self._create_experiment_unit(args, role)

    async def launch():
      experiment_unit = await experiment_unit_future
      await experiment_unit.add(job, args)
      return experiment_unit

    return asyncio.wrap_future(self._create_task(launch()))

  @abc.abstractmethod
  def _create_experiment_unit(
      self, args: Optional[Mapping[str, Any]],
      role: ExperimentUnitRole) -> Awaitable[ExperimentUnit]:
    """Creates a new experiment unit.

    Synchronously starts the experiment unit creation, ensuring that IDs would
    be assigned in invocation order. The operation itself may run asynchronously
    in background.

    Args:
      args: Executable unit arguments, to be show as a part of hyper-parameter
        sweep.
      role: Executable unit role: whether to create a work or auxiliary unit.

    Returns:
      An awaitable to the creation result.
    """
    raise NotImplementedError

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
    """Returns metadata context for the experiment."""
    return metadata_context.MetadataContext(
        creator=getpass.getuser(),
        annotations=metadata_context.ContextAnnotations())


@abc.abstractmethod
def create_experiment(experiment_title: Optional[str] = None) -> Experiment:
  """Returns a concrete Experiment instance."""
  raise NotImplementedError


@abc.abstractmethod
def get_experiment(experiment_id: int) -> Experiment:
  """Returns an Experiment instance associated with this experiment id.

  Args:
    experiment_id: An ID of an experiment to get.

  Raises:
    NotFoundError: If experiment is not found.
  """
  raise NotImplementedError
