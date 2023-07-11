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
from typing import Callable, List, Sequence, Tuple

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
  """A constraint with the list of jobs it applies to."""

  constraint: job_blocks.Constraint
  jobs: List[job_blocks.Job]


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

  def matcher(
      job_type: job_blocks.JobTypeVar,
  ) -> Tuple[List[ConstraintClique], List[job_blocks.Job]]:
    match job_type:
      case job_blocks.Job() as job:
        return [], [job]  # pytype: disable=bad-return-type
      case job_blocks.JobGroup() as job_group:
        cliques: List[ConstraintClique] = []
        jobs: List[job_blocks.Job] = []
        for job in job_group.jobs.values():
          subcliques, subjobs = matcher(job)  # pylint: disable=unpacking-non-sequence
          cliques += subcliques
          jobs += subjobs
        cliques = [
            ConstraintClique(constraint, jobs)
            for constraint in job_group.constraints
        ] + cliques
        return cliques, jobs
      case _:
        raise TypeError(f'Unsupported job_type: {job_type!r}')

  result, _ = matcher(job_group)  # pylint: disable=unpacking-non-sequence
  return result


def flatten_jobs(job_group: job_blocks.JobGroup) -> List[job_blocks.Job]:
  return collect_jobs_by_filter(job_group, lambda _: True)
