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
import collections
from concurrent import futures
import contextvars
import enum
import getpass
import inspect
import queue
import sys
import threading
import traceback
from typing import Any, Awaitable, Callable, ClassVar, Collection, Coroutine, Counter, Dict, Generator, List, Mapping, Optional, Sequence, Type, overload

from absl import logging
import attr
from typing_extensions import Self
from xmanager.xm import async_packager
from xmanager.xm import id_predictor
from xmanager.xm import job_blocks
from xmanager.xm import job_operators
from xmanager.xm import metadata_context

# ContextVars holding the current experiment (when within an Experiment context)
# and current experiment unit (when inside a JobGenerator).
_current_experiment: contextvars.ContextVar['Experiment'] = (
    contextvars.ContextVar('_xm_current_experiment')
)
_current_experiment_unit: contextvars.ContextVar['ExperimentUnit'] = (
    contextvars.ContextVar('_xm_current_experiment_unit')
)


def _check_if_unsupported_args_are_present(
    args: Mapping[str, Any], supported_args: Collection[str], job_type: str
) -> None:
  supported_args = set(supported_args)
  unsupported_args = set(args.keys()) - supported_args
  if unsupported_args:
    raise ValueError(
        f'Arguments {unsupported_args!r} are not supported by {job_type}. Only '
        f'{supported_args!r} are allowed.'
    )


def _apply_args(job_type: job_blocks.JobType, args: Mapping[str, Any]) -> None:
  # pytype: disable=attribute-error
  match job_type:
    case job_blocks.Job() as job:
      _check_if_unsupported_args_are_present(
          args, ('args', 'env_vars'), 'xm.Job'
      )

      if 'args' in args:
        job.args = job_blocks.merge_args(job.args, args['args'])
      if 'env_vars' in args:
        job.env_vars = job.env_vars.copy()
        job.env_vars.update(args['env_vars'])
    case job_blocks.JobGroup() as job_group:
      if args:
        _check_if_unsupported_args_are_present(
            args, job_group.jobs.keys(), 'xm.JobGroup'
        )
        for key, job in job_group.jobs.items():
          _apply_args(job, args.get(key, {}))
    case _:
      pass
  # pytype: enable=attribute-error


def _is_coro_context() -> bool:
  """Returns whether we are currently running in a coroutine context."""
  is_coro_context = False
  try:
    asyncio.get_running_loop()
    is_coro_context = True
  except RuntimeError:
    pass
  return is_coro_context


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
  """Experiment unit could not be completed.

  Attrs:
    work_unit: The work unit in which the error occurred, if available.
  """

  work_unit: Optional['WorkUnit'] = None

  def __init__(self, message: Any, *, work_unit: Optional['WorkUnit'] = None):
    super().__init__(message)
    self.work_unit = work_unit


class ExperimentUnitFailedError(ExperimentUnitError):
  """A job running in an experiment unit has failed."""


class ExperimentUnitNotCompletedError(ExperimentUnitError):
  """Experiment unit is neither running nor completed.

  For example it may be stopped by a user.
  """


class NotFoundError(KeyError):
  """Experiment/Work Unit/etc. has not been found."""


class ReloadError(ExperimentUnitError):
  """Raised when an XReload reload check fails during the reload step."""


class DebugInterrupt(BaseException):  # pylint: disable=g-bad-exception-name
  """Raised when a debug interrupt is requested."""


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

  def deduce_args(job_type: job_blocks.JobType) -> Dict[str, Any]:
    # pytype: disable=attribute-error
    match job_type:
      case job_blocks.Job() as job:
        args = {
            'args': job.args.to_dict(kwargs_only=True),
            'env_vars': job.env_vars,
        }
        return {key: value for key, value in args.items() if value}
      case job_blocks.JobGroup() as job_group:
        args = {}
        for job_name, job in job_group.jobs.items():
          job_args = deduce_args(job)
          if job_args:
            args[job_name] = job_args
        return args
      case _:
        return {}
    # pytype: enable=attribute-error

  return deduce_args(job)


@attr.s(auto_attribs=True, kw_only=True)
class LaunchedJob:
  """A read-only view of a launched job.

  Launched jobs correspond to job instances that have been added to an
  Experiment Unit. If a Job does not have a unique identity, it can be added
  multiple times to an Experiment, so a Job object may correspond to multiple
  LaunchedJob objects. A LaunchedJob will have the same name as the added Job,
  but launched jobs may have different addresses and logs.

  `experiment.add(job)` will generate an Experiment Unit
  containing a corresponding launched job. `experiment.add(job_group)` will
  generate an Experiment Unit with multiple corresponding launched jobs.
  `experiment.add(generator)` will generate an Experiment Unit with as many
  launched jobs as the generator adds.

  Attributes:
    name: Name of the job corresponding to this launched job.
    address: The job's address.
    logs: A URL to this job's logs.
  """

  name: str
  address: Optional[str] = None
  logs: Optional[str] = None


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

  experiment: 'Experiment'

  def __init__(
      self,
      experiment: 'Experiment',
      create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
      args: Optional[Mapping[str, Any]],
      role: ExperimentUnitRole,
      identity: str = '',
  ) -> None:
    """Initializes an `ExperimentUnit` instance.

    Args:
      experiment: An experiment this unit belongs to.
      create_task: A callback to register a new asynchronous task.
      args: Arguments to this experiment unit. Most commonly used to represent
        the hyperparameter sweep trial corresponding to a work unit.
      role: The role of this unit in the experiment structure.
      identity: The unique (user defined) identifier for this work unit.
    """
    self.experiment = experiment
    self._create_task = create_task
    self._args = args
    self._role = role
    self._identity = identity

    self._launch_tasks: List[futures.Future[Any]] = []

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    return self.experiment.experiment_id

  @property
  def identity(self) -> str:
    """Returns the unique identity (user assigned) for the experiment unit."""
    return self._identity

  def add(
      self,
      job: job_blocks.JobType,
      args: Optional[Mapping[str, Any]] = None,
      *,
      identity: str = '',
  ) -> Awaitable[None]:
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
      identity: Optional unique job identifier. If not empty, `add` adopts an
        'add if not exists' behavior. No changes to existing job would be done.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    # pyformat: enable

    # Prioritize the identity given directly to the work unit at work unit
    # creation time, as opposed to the identity passed when adding jobs to it as
    # this is more consistent between job generator work units and regular work
    # units.
    identity = self.identity or identity

    job = job_operators.shallow_copy_job_type(job)
    if args is not None:
      _apply_args(job, args)
    job_operators.populate_job_names(job)

    def launch_job(job: job_blocks.Job) -> Awaitable[None]:
      _current_experiment.set(self.experiment)
      _current_experiment_unit.set(self)
      return self._launch_job_group(
          job_blocks.JobGroup(**{job.name: job}),
          _work_unit_arguments(job, self._args),
          identity,
      )

    def launch_job_group(group: job_blocks.JobGroup) -> Awaitable[None]:
      _current_experiment.set(self.experiment)
      _current_experiment_unit.set(self)
      return self._launch_job_group(
          group, _work_unit_arguments(group, self._args), identity
      )

    def launch_job_generator(
        job_generator: job_blocks.JobGeneratorType,
    ) -> Awaitable[None]:
      if not inspect.iscoroutinefunction(
          job_generator
      ) and not inspect.iscoroutinefunction(job_generator.__call__):
        raise ValueError(
            'Job generator must be an async function. Signature needs to be '
            '`async def job_generator(work_unit: xm.WorkUnit) -> None:`'
        )
      _current_experiment.set(self.experiment)
      _current_experiment_unit.set(self)
      coroutine = job_generator(self, **(args or {}))
      assert coroutine is not None
      return coroutine

    def launch_job_config(job_config: job_blocks.JobConfig) -> Awaitable[None]:
      _current_experiment.set(self.experiment)
      _current_experiment_unit.set(self)
      return self._launch_job_config(job_config, args or {}, identity)

    job_awaitable: Awaitable[Any]
    match job:
      case job_blocks.Job() as job:
        job_awaitable = launch_job(job)
      case job_blocks.JobGroup() as job_group:
        job_awaitable = launch_job_group(job_group)
      case job_generator if job_blocks.is_job_generator(job):
        job_awaitable = launch_job_generator(job_generator)
      case job_blocks.JobConfig() as job_config:
        job_awaitable = launch_job_config(job_config)
      case _:
        raise TypeError(f'Unsupported job type: {job!r}')

    launch_task = self._create_task(job_awaitable)
    self._launch_tasks.append(launch_task)
    return asyncio.wrap_future(launch_task)

  async def _wait_until_complete_impl(self) -> Self:
    try:
      for task in self._launch_tasks:
        await asyncio.wrap_future(task)
    except Exception as e:
      raise ExperimentUnitError('Experiment unit could not be created.') from e
    await self._wait_until_complete()
    return self

  def wait_until_complete(self) -> Coroutine[Any, Any, 'ExperimentUnit']:
    """Waits until the unit is in a final state: completed/failed/stopped.

    Raises:
      ExperimentUnitError: Exception if the unit couldn't complete.
    Returns:
      Returns self to facilitate asyncio.as_completed usage.
    """
    return self._wait_until_complete_impl()

  async def _launch_job_group(
      self,
      job_group: job_blocks.JobGroup,
      args_view: Mapping[str, Any],
      identity: str,
  ) -> None:
    """Launches a given job group as part of the unit."""
    raise NotImplementedError

  async def _launch_job_config(
      self,
      job_config: job_blocks.JobConfig,
      args_view: Mapping[str, Any],
      identity: str,
  ) -> None:
    """Launches a given job config as part of the unit."""
    raise NotImplementedError

  async def _wait_until_complete(self) -> None:
    """Waits until the unit is in a final state: completed/failed/stopped.

    Child classes need to implement this method to support awaiting units.

    Unlike wait_until_complete this method asumes that unit has been fully
    created. This method is only invoked if somebody has requested to monitor
    unit.
    """
    raise NotImplementedError

  def stop(
      self,
      *,
      mark_as_failed: bool = False,
      mark_as_completed: bool = False,
      message: Optional[str] = None,
  ) -> None:
    """Initiate the process to stop the unit from running.

    This method will synchronously make a request for the unit to stop.
    However, the method does not actually wait for the unit to be in a
    terminal state.

    Use self.wait_until_complete() after self.stop() to guarantee the unit
    is stopped.

    Args:
      mark_as_failed: Mark this unit as failed rather than stopped.
      mark_as_completed: Mark this unit as completed rather than stopped.
      message: Optional user-defined status message.
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
        annotations=metadata_context.ContextAnnotations(),
    )

  @property
  def launched_jobs(self) -> List[LaunchedJob]:
    """Gets a representation for each individual job that was added.

    Each added Job should produce a LaunchedJob.
    Each added JobGroup will produce a LaunchedJob for each leaf Job.
    """
    raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True)
class WorkUnitRole(ExperimentUnitRole):
  """An experiment unit with this role is a work unit.

  Work units contain jobs that are often run as trials as part of an
  experiment's hyper-parameter search. The status of a work unit is used to
  determine the status of the experiment.
  """


class WorkUnitCompletedAwaitable(Coroutine):
  """Awaitable for work unit completion event.

  Usage:
    completion_events = [work_unit.wait_until_complete() for work_unit in ...]
    while completion_events:
      completed_event, completion_events = asyncio.wait(
          completion_events, return_when=asyncio.FIRST_COMPLETED)
      for event in completed_events:
        wid = event.work_unit.work_unit_id
        try:
          await event
          print(f'Work unit {wid} completed successfully.')
        except xm.ExperimentUnitError as e:
          print(f'Work unit {wid} failed: {e}.')
  """

  def __init__(
      self, work_unit: 'WorkUnit', awaitable: Callable[[], Any]
  ) -> None:
    self.work_unit = work_unit
    self._awaitable = awaitable
    self._wait_coro = self._wait()

  async def _wait(self) -> 'WorkUnit':
    # Coroutine must be created inside of async function to avoid
    # "coroutine ... was never awaited" runtime warning.
    await self._awaitable()
    return self.work_unit

  def __await__(self) -> Generator[Any, None, 'WorkUnit']:
    return self._wait_coro.__await__()

  def send(self, value: Any) -> Any:
    return self._wait_coro.send(value)

  def throw(self, typ, val=None, tb=None) -> Any:
    return self._wait_coro.throw(typ, val, tb)

  def close(self) -> None:
    self._wait_coro.close()


class WorkUnit(ExperimentUnit):
  """Work units are experiment units with the work unit role."""

  @property
  @abc.abstractmethod
  def work_unit_id(self) -> int:
    raise NotImplementedError

  def wait_until_complete(self) -> WorkUnitCompletedAwaitable:
    """Waits until the unit is in a final state: completed/failed/stopped.

    Raises:
      ExperimentUnitError: Exception if the unit couldn't complete.
    Returns:
      Returns self to facilitate asyncio.as_completed usage.
    """
    return WorkUnitCompletedAwaitable(self, self._wait_until_complete_impl)


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


class AuxiliaryUnitJob(abc.ABC):
  """A job bundled with an AuxiliaryUnitRole.

  This class allows libraries to define self-contained objects which would
  result in AUX units once added to the experiment.
  Note that this class conforms to xm.JobGenerator interface.
  """

  role: AuxiliaryUnitRole
  _job: job_blocks.JobType

  def __init__(
      self,
      job: job_blocks.JobType,
      *,
      importance: Importance = Importance.NORMAL,
      termination_delay_secs: int,
  ) -> None:
    self.role = AuxiliaryUnitRole(
        importance=importance,
        termination_delay_secs=termination_delay_secs,
    )
    self._job = job

  async def __call__(self, aux_unit: ExperimentUnit, **kwargs):
    if not job_blocks.is_job_generator(self._job):
      aux_unit.add(self._job, args=kwargs)
      return

    job_generator = self._job
    coroutine = job_generator(aux_unit, **kwargs)
    assert coroutine is not None
    await coroutine


class Experiment(abc.ABC):
  """Experiment is the core unit of research in XManager.

  An experiment typically involves running a computation (e.g. training a model)
  in different hyperparameter configurations. Experiments are made up of work
  units, which do the computation(s) in question, and auxiliary units which
  perform other functions like TensorBoard. It can have associated metadata,
  such as title or source code commit from which it has been compiled.

  XManager API provides multiple implementations of the Experiment class which
  use different backends to start jobs. Types of supported Executables may vary
  between these implementations. Usually experiments woulbe be created through
  xm_<foo>.create_experiment() functions.

  While experiment metadata and statuses can be accessed and altered directly,
  adding work or auxiliary units requires entering experiment context:

    with xm_foo.create_experiment(...) as experiment:
      experiment.add(xm.Job(...))

  This context would spawn background event pool to support asynchronous
  operations and ensure that they all are completed on exit. It also may mark
  experiment as failed if an exception is thrown.
  """

  # An event loop in which job generators would be run.
  _event_loop: asyncio.AbstractEventLoop
  # A queue of background tasks that launch work units.
  _running_tasks: queue.Queue[futures.Future[Any]]
  # Work unit ID predictor.
  _work_unit_id_predictor: id_predictor.Predictor
  # A class variable for batching packaging requests.
  _async_packager: ClassVar[async_packager.AsyncPackager]
  # ContextVars token when entering the context.
  _current_experiment_token: contextvars.Token
  _current_async_experiment_token: contextvars.Token
  # Counts how many of each role were added to the experiment.
  _added_roles: Counter[Type[ExperimentUnitRole]] = collections.Counter()

  @property
  def experiment_id(self) -> int:
    """Returns a unique ID assigned to the experiment."""
    raise NotImplementedError

  def __enter__(self) -> Self:
    if _is_coro_context():
      raise RuntimeError(
          'When using Experiment from a coroutine please use '
          '`async with` syntax'
      )

    self._current_experiment_token = _current_experiment.set(self)
    self._event_loop = asyncio.new_event_loop()
    self._event_loop_thread = self._thread_factory()(
        target=self._event_loop.run_forever, daemon=True
    )
    self._event_loop_thread.start()

    # asyncio.run_coroutine_threadsafe doesn't accept class method and wants it
    # wrapped in a function.
    async def async_enter():
      await self.__aenter__()

    asyncio.run_coroutine_threadsafe(
        async_enter(), loop=self._event_loop
    ).result()

    return self

  def _thread_factory(self) -> type[threading.Thread]:
    """Gets the type of Thread to use for asynchronous operations.

    Defaults to `threading.Thread`.

    Returns:
      The type to use for constructing a Thread.
    """
    return threading.Thread

  def _wait_for_tasks(self):
    """Waits for pending tasks to complete, raising the first error."""
    exception = None
    while not self._running_tasks.empty():
      try:
        self._running_tasks.get_nowait().result()
      except futures.CancelledError:
        # Ignore cancelled tasks.
        pass
      except Exception as e:  # pylint: disable=broad-except
        # Allow remaining tasks to complete before raising the first exception.
        if not exception:
          exception = e
    if exception:
      raise exception

  def __exit__(self, exc_type, exc_value, traceback):  # pylint:disable=redefined-outer-name
    _current_experiment.reset(self._current_experiment_token)
    self._wait_for_tasks()
    self._event_loop.call_soon_threadsafe(self._event_loop.stop)
    self._event_loop_thread.join()
    self._validate_added_roles()

  async def __aenter__(self) -> Self:
    self._current_async_experiment_token = _current_experiment.set(self)
    self._event_loop = asyncio.get_event_loop()
    self._running_tasks = queue.Queue()
    self._work_unit_id_predictor = id_predictor.Predictor(
        1 + self.work_unit_count
    )
    return self

  async def _await_for_tasks(self):
    while not self._running_tasks.empty():
      await asyncio.wrap_future(self._running_tasks.get_nowait())

  async def __aexit__(self, exc_type, exc_value, traceback):  # pylint:disable=redefined-outer-name
    _current_experiment.reset(self._current_async_experiment_token)
    await self._await_for_tasks()
    self._validate_added_roles()

  def _validate_added_roles(self):
    """Validates the roles added to this experiment.

    By default, logs a warning if no work units were added, which is usually
    unintentional.
    """
    if not self._added_roles[WorkUnitRole]:
      logging.warning(
          'No work units were added to this experiment, which is usually not'
          ' intended.'
      )

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
      cls, packageable: job_blocks.Packageable
  ) -> Awaitable[job_blocks.Executable]:
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
  def add(
      self,
      job: AuxiliaryUnitJob,
      args: Optional[Mapping[str, Any]] = ...,
      *,  # parameters after “*” are keyword-only parameters
      identity: str = '',
  ) -> asyncio.Future[ExperimentUnit]:
    ...

  @overload
  def add(
      self,
      job: job_blocks.JobType,
      args: Optional[Mapping[str, Any]] = ...,
      *,  # parameters after “*” are keyword-only parameters
      role: WorkUnitRole = ...,
      identity: str = '',
  ) -> asyncio.Future[WorkUnit]:
    ...

  @overload
  def add(
      self,
      job: job_blocks.JobType,
      args: Optional[Mapping[str, Any]],
      *,  # parameters after “*” are keyword-only parameters
      role: ExperimentUnitRole,
      identity: str = '',
  ) -> asyncio.Future[ExperimentUnit]:
    ...

  @overload
  def add(
      self,
      job: job_blocks.JobType,
      args: Optional[Mapping[str, Any]] = ...,
      *,  # parameters after “*” are keyword-only parameters
      role: ExperimentUnitRole,
      identity: str = '',
  ) -> asyncio.Future[ExperimentUnit]:
    ...

  # The ExperimentUnit return type is determined by the role.
  def add(self, job, args=None, *, role=WorkUnitRole(), identity: str = ''):
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
      identity: Optional unique experiment unit identifier within the
        experiment. If not empty, `add` adopts an 'add if not exists' behavior.
        If a unit with the given identity already exists it will be returned as
        is, without modifications. JobGenerators would still run to allow them
        to recover after preemption.

    Returns:
      An awaitable that would be fulfilled when the job is launched.
    """
    # pyformat: enable
    if isinstance(job, AuxiliaryUnitJob):
      role = job.role
    self._added_roles[type(role)] += 1

    if self._should_reload_experiment_unit(role):
      experiment_unit_future = self._get_experiment_unit(
          self.experiment_id, identity, role, args
      )
    else:
      experiment_unit_future = self._create_experiment_unit(
          args, role, identity
      )

    async def launch():
      experiment_unit = await experiment_unit_future
      try:
        await experiment_unit.add(job, args, identity=identity)
      except Exception as experiment_exception:
        logging.error(
            'Stopping experiment unit (identity %r) after it failed with: %s',
            identity,
            experiment_exception,
        )
        try:
          if isinstance(job, AuxiliaryUnitJob):
            experiment_unit.stop()
          else:
            experiment_unit.stop(
                mark_as_failed=True,
                message=f'Work unit creation failed. {traceback.format_exc()}',
            )
        except Exception as stop_exception:  # pylint: disable=broad-except
          logging.error("Couldn't stop experiment unit: %s", stop_exception)
        raise
      except DebugInterrupt:
        try:
          experiment_unit.stop(message='Launch interrupted by debug mode.')
        except Exception as stop_exception:  # pylint: disable=broad-except
          logging.error("Couldn't stop experiment unit: %s", stop_exception)
      return experiment_unit

    async def reload():
      experiment_unit = await experiment_unit_future
      try:
        await experiment_unit.add(job, args, identity=identity)
      except Exception as update_exception:
        logging.error(
            'Could not reload the experiment unit: %s',
            update_exception,
        )
        raise
      return experiment_unit

    return asyncio.wrap_future(
        self._create_task(
            reload() if self._should_reload_experiment_unit(role) else launch()
        ),
        loop=self._event_loop,
    )

  @abc.abstractmethod
  def _get_experiment_unit(
      self,
      experiment_id: int,
      identity: str,
      role: ExperimentUnitRole,
      args: Optional[Mapping[str, Any]] = None,
  ) -> Awaitable[ExperimentUnit]:
    """Returns an existing experiment unit by identity.

    Args:
      experiment_id: The ID of the experiment to get the Experiment Unit for.
      identity: The identity of the Experiment Unit to get.
      role: Executable unit role: whether to fetch a work unit or auxiliary
        unit.
      args: Keyword arguments to be passed to the job.

    Returns:
      An awaitable which fetches the work unit.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def _create_experiment_unit(
      self,
      args: Optional[Mapping[str, Any]],
      role: ExperimentUnitRole,
      identity: str,
  ) -> Awaitable[ExperimentUnit]:
    """Creates a new experiment unit.

    Synchronously starts the experiment unit creation, ensuring that IDs would
    be assigned in invocation order. The operation itself may run asynchronously
    in background.

    Args:
      args: Executable unit arguments, to be show as a part of hyper-parameter
        sweep.
      role: Executable unit role: whether to create a work or auxiliary unit.
      identity: Optional user-given experiment unit id.

    Returns:
      An awaitable to the creation result.
    """
    raise NotImplementedError

  def _create_task(self, task: Awaitable[Any]) -> futures.Future[Any]:
    if not self._event_loop.is_running():
      raise RuntimeError(
          'Event loop is not running. Have you entered Experiment context '
          'manager (e.g. with xm.create-experiment() as experiment:)?'
      )
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
        annotations=metadata_context.ContextAnnotations(),
    )

  @abc.abstractmethod
  def _should_reload_experiment_unit(self, role: ExperimentUnitRole) -> bool:
    """Returns True if the experiment unit should be reloaded based on its role.

    Reloading an experiment depends on the context in which it is running in.
    Primarily it entails updating, stopping, and restarting the executable
    units very quickly without having to wait for scheduling.

    Args:
      role: Experiment unit role trying to be reloaded.
    """
    raise NotImplementedError


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
