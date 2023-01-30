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
"""Data classes for job-related abstractions."""

import abc
import functools
import itertools
import re
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, TypeVar, Union

from absl import flags as absl_flags
import attr
from xmanager.xm import pattern_matching
from xmanager.xm import utils

UserArgs = Union[Mapping, Sequence, 'SequentialArgs']

_ENABLE_MULTI_ARG_FLAGS = absl_flags.DEFINE_bool(
    'xm_to_list_multi_arg_behavior',
    False,
    'If True, pass `args=dict(x=[v0, v1])` as --x=v0, --x=v1',
)


@functools.cache
def print_none_warning(key: str) -> None:
  print(
      f'WARNING: Setting `{key}=None` will exclude the flag. To pass the '
      f'actual value, pass the string literal `{key}="None"` instead'
  )


def _is_nested_structure(structure: Union[List[Any], Tuple[Any]]) -> bool:
  """Returns true if a list or tuple contains a list or tuple."""
  return any(type(element) in (list, tuple) for element in structure)


class SequentialArgs:
  """A sequence of positional and keyword arguments for a binary.

  Unix command line arguments are just a list of strings. But it is very common
  to simulate keyword arguments in a --key=value form. It is not uncommon to
  only have keyword arguments. Therefore we allow providing args as:

  Dicts:
    {'foo': 'space bar', 'with_magic': True} -> --foo='space bar' --with_magic
    Argument order is preserved.
  Lists:
    ['--foo', 'space bar'] -> --foo 'space bar'
  SequentialArgs (which allows to represent a mix of the two above):
    xm.merge_args({'foo': 'bar'}, ['--'], {'n': 16}) -> --foo=bar -- --n=16

  SequentialArgs provides a convenient merging semantics: if a value is given
  for an existing keyword argument, it will be overriden rather than appended,
  which allows to specify default values and override them later:

    xm.merge_args({'foo': '1', 'bar': '42'}, {'foo': '2'}) -> --foo=2 --bar=42

  SequentialArgs is immutable, but you can get a copy with updated value:

    args = xm.merge_args({'foo': '1', 'bar': '42'})
    args = xm.merge_args(args, {'foo': '2'})

  We only allow appending new arguments (positional and keyword) and overriding
  keyword arguments. Removal and inserting to the middle is not supported.
  """

  @attr.s(auto_attribs=True)
  class _RegularItem:
    value: Any

  @attr.s(auto_attribs=True)
  class _KeywordItem:
    name: str

  def __init__(self) -> None:
    """Constucts an empty SequentialArgs.

    Prefer using xm.merge_args to construct SequentialArgs objects.
    """
    self._items: List[
        Union[SequentialArgs._RegularItem, SequentialArgs._KeywordItem]
    ] = []
    self._kwvalues: Dict[str, Any] = {}

  def _ingest_regular_item(self, value: Any) -> None:
    self._items.append(SequentialArgs._RegularItem(value))

  def _ingest_keyword_item(self, name: str, value: Any) -> None:
    if name not in self._kwvalues:
      self._items.append(SequentialArgs._KeywordItem(name))
    self._kwvalues[name] = value

  def _merge_from(self, args: 'SequentialArgs') -> None:
    """Merges another instance of SequentialArgs into self."""

    def import_regular_item(item: SequentialArgs._RegularItem):
      self._ingest_regular_item(item.value)

    def import_keyword_item(item: SequentialArgs._KeywordItem):
      self._ingest_keyword_item(item.name, args._kwvalues[item.name])  # pylint: disable=protected-access

    matcher = pattern_matching.match(
        import_regular_item,
        import_keyword_item,
    )
    for item in args._items:  # pylint: disable=protected-access
      matcher(item)

  @staticmethod
  def from_collection(collection: Optional[UserArgs]) -> 'SequentialArgs':
    """Populates a new instance from a given collection."""
    result = SequentialArgs()
    if collection is None:
      return result

    def check_for_string(args: str) -> None:
      raise ValueError(
          f'Tried to construct xm.SequentialArgs from a string: {args!r}. '
          f'Wrap it in a list: [{args!r}] to make it a single argument.'
      )

    def import_sequential_args(args: SequentialArgs) -> None:
      result._merge_from(args)  # pylint: disable=protected-access

    def import_mapping(collection: Mapping[Any, Any]) -> None:
      for key, value in collection.items():
        result._ingest_keyword_item(str(key), value)  # pylint: disable=protected-access

    def import_sequence(collection: Sequence[Any]) -> None:
      for value in collection:
        result._ingest_regular_item(value)  # pylint: disable=protected-access

    matcher = pattern_matching.match(
        check_for_string,
        import_sequential_args,
        import_mapping,
        import_sequence,
    )
    matcher(collection)
    return result

  def rewrite_args(self, rewrite: Callable[[str], str]) -> 'SequentialArgs':
    """Applies the rewrite function to all args and returns the result."""
    result = SequentialArgs()

    def rewrite_regular_item(item: SequentialArgs._RegularItem):
      new_value = item.value
      if isinstance(new_value, str):
        new_value = rewrite(new_value)
      result._ingest_regular_item(new_value)  # pylint: disable=protected-access

    def rewrite_keyword_item(item: SequentialArgs._KeywordItem):
      new_value = self._kwvalues[item.name]
      if isinstance(new_value, str):
        new_value = rewrite(new_value)
      result._ingest_keyword_item(item.name, new_value)  # pylint: disable=protected-access

    matcher = pattern_matching.match(
        rewrite_regular_item,
        rewrite_keyword_item,
    )
    for item in self._items:
      matcher(item)

    return result

  def to_list(
      self,
      escaper: Callable[[Any], str] = utils.ARG_ESCAPER,
      kwargs_joiner: Callable[[str, str], str] = utils.trivial_kwargs_joiner,
  ) -> List[str]:
    """Exports items as a list ready to be passed into the command line."""

    def export_regular_item(
        item: SequentialArgs._RegularItem,
    ) -> List[Optional[str]]:
      return [escaper(item.value)]

    def export_keyword_item(
        item: SequentialArgs._KeywordItem,
    ) -> List[Optional[str]]:
      value = self._kwvalues[item.name]
      if value is None:
        # We skip flags with None value, allowing the binary to use defaults.
        # A string can be used if a literal "None" value needs to be assigned.
        print_none_warning(item.name)
        return [None]
      elif isinstance(value, bool):
        return [escaper(f"--{'' if value else 'no'}{item.name}")]
      elif type(value) in (list, tuple) and not _is_nested_structure(value):
        # TODO: Cleanup once users have migrated
        if _ENABLE_MULTI_ARG_FLAGS.value:
          # Pass sequence of arguments in by repeating the flag for each
          # element to be consistent with absl's handling of multiple flags.
          # We do not do this for nested sequences, which absl cannot handle,
          # and instead fallback to quoting the sequence and leaving parsing of
          # the nested structure to the executable being called.
          return [
              kwargs_joiner(escaper(f'--{item.name}'), escaper(v))
              for v in value
          ]
        else:
          print(
              '*****BREAKAGE WARNING: Passing `args=dict(flag=[v0, v1])` will'
              ' change behavior on 2023/01/15 to pass args as `--flag=v0'
              ' --flag=v1` instead of `--flag=[v0, v1]` as currently. If'
              ' `--flag` is parsed using `ml_collections` and overrides a'
              ' tuple field, no action is required. Otherwise, to keep the old'
              ' behavior, simply wrap your list in `str`:'
              ' `args=dict(flag=str([v0, v1,...]))`.\nThe new behavior is more'
              ' consistent:\n `flags.DEFINE_multi_xyz`: `args=dict(x=[v0,'
              ' v1])` match `FLAGS.x == [v0, v1]`.\n `flags.DEFINE_string`:'
              " `args=dict(x='[v0, v1]')` match `FLAGS.x == '[v0, v1]'`.\n***"
              f' Impacted args: --{item.name}={str(value):.20} ***'
          )
          return [kwargs_joiner(escaper(f'--{item.name}'), escaper(value))]
      else:
        return [kwargs_joiner(escaper(f'--{item.name}'), escaper(value))]

    matcher = pattern_matching.match(
        export_regular_item,
        export_keyword_item,
    )
    flags = itertools.chain.from_iterable(matcher(item) for item in self._items)
    return [f for f in flags if f is not None]

  def to_dict(self, kwargs_only: bool = False) -> Dict[str, Any]:
    """Exports items as a dictionary.

    Args:
      kwargs_only: Whether to skip positional arguments.

    Returns:
      The sought dictionary.
    """
    if kwargs_only:
      return self._kwvalues

    def export_regular_item(
        item: SequentialArgs._RegularItem,
    ) -> Tuple[str, Any]:
      return (str(item.value), True)

    def export_keyword_item(
        item: SequentialArgs._KeywordItem,
    ) -> Tuple[str, Any]:
      return (item.name, self._kwvalues[item.name])

    matcher = pattern_matching.match(
        export_regular_item,
        export_keyword_item,
    )
    return dict([matcher(item) for item in self._items])

  def __eq__(self, other) -> bool:
    return isinstance(other, SequentialArgs) and all([
        self._items == other._items,
        self._kwvalues == other._kwvalues,
    ])

  def __repr__(self) -> str:
    return f"[{', '.join(self.to_list(repr))}]"


def merge_args(*operands: Union[SequentialArgs, UserArgs]) -> SequentialArgs:
  """Merges several arguments collections into one left-to-right."""
  result = SequentialArgs()
  for operand in operands:
    if not isinstance(operand, SequentialArgs):
      operand = SequentialArgs.from_collection(operand)
    result._merge_from(operand)  # pylint: disable=protected-access
  return result


class ExecutableSpec(abc.ABC):
  """Executable specification describes what code / computation to run.

  Use one of the functions declared in xm/packagables.py to create a spec:
   * xm.binary - a prebuilt executable program.
   * xm.bazel_binary - an executable built with Bazel.
   * xm.container - a prebuilt Docker container.
   * xm.bazel_container - a Docker container built with Bazel.
   * xm.python_container - a Docker container running python code.
   * xm.dockerfile_container - a Docker container built with dockerfile.

  An executable spec must be turned into an Executable using
  Experiment.package() in order to be used in a Job.

  WARNING: `ExecutableSpec`s are supposed to be implementation-agnostic. That
  means there should be no backend-specific class inheriting `ExecutableSpec`.
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
    name: An automatically populated name for the executable. Used for assigning
      default names to `Job`s.
  """

  name: str


class ExecutorSpec(abc.ABC):
  """Executor spec describes the location of the runtime environment.

  For a list of supported ExecutorSpecs see a list of executors below.
  """


class Executor(abc.ABC):
  """Executor describes the runtime environment of a Job.

  Concrete supported executors are listed in xm_local/executors.py:
    * xm_local.Local
    * xm_local.Vertex
    * xm_local.Kubernetes
  """

  @classmethod
  @abc.abstractmethod
  def Spec(cls) -> ExecutorSpec:  # pylint: disable=invalid-name
    raise NotImplementedError


def _validate_env_vars(
    self: Any, attribute: Any, env_vars: Dict[str, str]
) -> None:
  del self  # Unused.
  del attribute  # Unused.
  for key in env_vars.keys():
    if not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]*', key):
      raise ValueError(
          'Environment variables names must conform to '
          f'[a-zA-Z_][a-zA-Z0-9_]*. Got {key!r}.'
      )


@attr.s(auto_attribs=True)
class Packageable:
  """Packageable describes what to build and its static parameters."""

  executable_spec: ExecutableSpec
  executor_spec: ExecutorSpec
  args: SequentialArgs = attr.ib(
      factory=list, converter=SequentialArgs.from_collection
  )  # pytype: disable=annotation-type-mismatch
  env_vars: Dict[str, str] = attr.ib(
      converter=dict, default=attr.Factory(dict), validator=_validate_env_vars
  )


class Constraint(abc.ABC):
  """Constraint describes the requirements for where a job group can run.

  Some examples of constraints include:

    * same virtual machine;
    * same virtual private Cloud subnetwork;
    * same network fabric;
    * same geographic location.
  """


# Job generators are async functions returning None.
# Pylint doesn't distinguish async and sync contexts so Optional[Awaitable] has
# to be used to accomodate both cases.
JobGeneratorType = Callable[..., Optional[Awaitable]]
JobType = Union['Job', 'JobGroup', JobGeneratorType, 'JobConfig']


@attr.s(auto_attribs=True)
class Job:
  """Job describes a unit of computation to be run.

  Attributes:
    executable: What to run -- one of `xm.Experiment.package` results.
    executor: Where to run -- one of `xm.Executor` subclasses.
    name: Name of the job. Must be unique within the context (work unit). By
      default it is constructed from the executable. Used for naming related
      entities such as newly created containers.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to apply.
  """

  executable: Executable
  executor: Executor
  name: Optional[str] = None
  args: SequentialArgs = attr.ib(
      factory=list, converter=SequentialArgs.from_collection
  )  # pytype: disable=annotation-type-mismatch
  env_vars: Dict[str, str] = attr.ib(
      converter=dict, default=attr.Factory(dict), validator=_validate_env_vars
  )


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
      constraints=[xm_impl.SameMachine()],
  )
  ```

  To express sophisticated requirements JobGroups can be nested:

  ```
  JobGroup(
      eval=Job(eval_executable, executor),
      colocated_learner_and_actor=JobGroup(
          learner=Job(tpu_learner_executable, executor),
          actor=Job(actor_executable, executor),
          constraints=[xm_impl.SameMachine()],
      ),
  )
  ```

  Attributes:
    jobs: A mapping of names to jobs.
    constraints: A list of additional scheduling constraints.
  """

  jobs: Dict[str, JobType]
  constraints: List[Constraint]

  def __init__(
      self,
      *,
      constraints: Optional[Sequence[Constraint]] = None,
      **jobs: JobType,
  ) -> None:
    """Builds a JobGroup.

    Args:
      constraints: List of additional scheduling constraints. Keyword only arg.
      **jobs: Jobs / job groups that constitute the group passed as kwargs.
    """
    self.jobs = jobs
    self.constraints = list(constraints) if constraints else []


class JobConfig(abc.ABC):
  """A job defined by a platform-specific configuration.

  Sometimes defining a job through a platform-agnostic xm.Job/xm.JobGroup
  interfaces is not feasible. In this case job can be defined by a configuration
  language native to the underlying platform. This is a base class for such
  configurations. Concrete XManager implementations may provide descendants for
  the configuration languages they support.
  """


JobTypeVar = TypeVar('JobTypeVar', Job, JobGroup, JobGeneratorType, JobConfig)


def get_args_for_all_jobs(job: JobType, args: Dict[str, Any]) -> Dict[str, Any]:
  """Gets args to apply on all jobs inside a JobGroup.

  This is useful if all jobs within a work unit accept the same arguments.

  Args:
    job: The job group to generate args for.
    args: The args to apply to all jobs inside the job group.

  Returns:
    args that can be added with work_unit.add()
  """
  if not isinstance(job, JobGroup):
    return {'args': dict(args)}
  all_args = {}
  for job_name, job_type in job.jobs.items():
    job_type_args = get_args_for_all_jobs(job_type, args)
    all_args[job_name] = job_type_args
  return all_args
