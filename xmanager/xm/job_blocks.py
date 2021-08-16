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
from typing import Awaitable, Callable, Dict, List, Optional, Sequence, Union

import attr

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


JobGeneratorType = Callable[..., Awaitable]
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
               constraints: Optional[Sequence[Constraint]] = None,
               **jobs: JobType) -> None:
    """Builds a JobGroup.

    Args:
      constraints: List of additional scheduling constraints. Keyword only arg.
      **jobs: Jobs / job groups that constitute the group passed as kwargs.
    """
    self.jobs = jobs
    self.constraints = list(constraints) if constraints else []
