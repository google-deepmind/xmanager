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
"""Common job operators useful in the framework and 3P-libraries."""

import copy
import itertools
from typing import Any, Callable, List, Sequence, Tuple

import attr
from xmanager.xm import job_blocks


def shallow_copy_job_type(
    job_type: job_blocks.JobTypeVar,
) -> job_blocks.JobTypeVar:
  """Creates a shallow copy of the job structure."""
  if job_blocks.is_job_generator(job_type):
    return job_type

  if isinstance(job_type, job_blocks.JobGroup):
    job_type = copy.copy(job_type)
    job_type.jobs = {
        key: shallow_copy_job_type(job) for key, job in job_type.jobs.items()
    }
    return job_type

  return copy.copy(job_type)


def populate_job_names(job_type: job_blocks.JobTypeVar) -> None:
  """Assigns default names to the given jobs."""

  def matcher(prefix: Sequence[str], job_type: job_blocks.JobTypeVar) -> None:
    match job_type:
      case job_blocks.Job() as target:
        if target.name is None:  # pytype: disable=attribute-error
          target.name = '_'.join(prefix) if prefix else target.executable.name
      case job_blocks.JobGroup() as target:
        for key, job in target.jobs.items():  # pytype: disable=attribute-error
          matcher([*prefix, key], job)
      case _:
        return

  matcher([], job_type)


def collect_jobs_by_filter(
    job_group: job_blocks.JobGroup,
    predicate: Callable[[job_blocks.Job], bool],
) -> List[job_blocks.Job]:
  """Flattens a given job group and filters the result."""

  def job_collector(job_type: job_blocks.JobTypeVar) -> List[job_blocks.Job]:
    match job_type:
      case job_blocks.Job() as job:
        return [job] if predicate(job) else []  # pytype: disable=bad-return-type
      case job_blocks.JobGroup() as job_group:
        return list(
            itertools.chain.from_iterable(
                [job_collector(job) for job in job_group.jobs.values()]
            )
        )
      case _:
        raise TypeError(f'Unsupported job_type: {job_type!r}')

  return job_collector(job_group)


@attr.s(auto_attribs=True)
class ConstraintClique:
  """A constraint with the job group it applies to.

  Attributes:
    constraint: The constraint that describes the requirements for where a job
      group can run.
    jobs: A list of direct jobs and all jobs in child JobGroups that the
      constraint applies to.
    group_name: The JobGroup name that the constraint applies to.
    size: The number of direct jobs and JobGroups that the constraint applies
      to.
    parent_group_name: The parent JobGroup name of the constraint.
  """

  constraint: job_blocks.Constraint
  jobs: List[job_blocks.Job]
  group_name: str | None = None
  size: int | None = None
  parent_group_name: str | None = None


def aggregate_constraint_cliques(
    job_group: job_blocks.JobGroup,
) -> List[ConstraintClique]:
  """Forms constraint cliques.

  For each constraint met, collects all jobs it applies to.

  Args:
    job_group: A job group to aggregate on.

  Returns:
    A set of cliques.
  """
  group_id = 0

  def construct_group_name(job_group: job_blocks.JobGroup) -> str:
    nonlocal group_id
    group_name = (
        '_'.join([name for name in job_group.jobs.keys()]) + '_' + str(group_id)
    )
    group_id += 1
    return group_name

  def matcher(
      job_type: job_blocks.JobTypeVar,
      parent_group_name: str | None,
  ) -> Tuple[List[ConstraintClique], List[job_blocks.Job]]:
    match job_type:
      case job_blocks.Job() as job:
        return [], [job]  # pytype: disable=bad-return-type
      case job_blocks.JobGroup() as job_group:
        cliques: List[ConstraintClique] = []
        jobs: List[job_blocks.Job] = []
        group_name = construct_group_name(job_group)
        size = len(job_group.jobs)
        for job in job_group.jobs.values():
          subcliques, subjobs = matcher(job, group_name)  # pylint: disable=unpacking-non-sequence
          cliques += subcliques
          jobs += subjobs
        cliques = [
            ConstraintClique(
                constraint=constraint,
                jobs=jobs,
                group_name=group_name,
                size=size,
                parent_group_name=parent_group_name,
            )
            for constraint in job_group.constraints
        ] + cliques
        return cliques, jobs
      case _:
        raise TypeError(f'Unsupported job_type: {job_type!r}')

  result, _ = matcher(job_group, None)  # pylint: disable=unpacking-non-sequence
  return result


def flatten_jobs(job_group: job_blocks.JobGroup) -> List[job_blocks.Job]:
  return collect_jobs_by_filter(job_group, lambda _: True)


def _check_job_exists(job_name: str, jobs: dict[str, Any]):
  """Raises a `ValueError` if the job exists in the jobs Dict."""
  if job_name in jobs:
    raise ValueError(f'{job_name} is duplicated in the experiment.')


def get_jobs(job_group: job_blocks.JobGroup) -> dict[str, job_blocks.JobType]:
  """Maps the jobs to their job name.

  Args:
    job_group: The jobs process.

  Returns:
    A map where the key is the job name and value is the job itself.
  """
  jobs = {}
  for key, value in job_group.jobs.items():
    match value:
      case job_blocks.Job():
        _check_job_exists(value.name, jobs)
        jobs[str(value.name)] = value
      case job_blocks.JobGroup():
        jobs.update(get_jobs(value))
      case _:
        # We treat JobGroups as a special case since we want to process the jobs
        # within them. Jobs *may* have a diffrerent name than key. All other
        # JobTypes are too generic to handle so we assume that the key is
        # representative.
        _check_job_exists(key, jobs)
        jobs[key] = value
  return jobs
