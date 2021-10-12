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
from xmanager.xm import pattern_matching


def shallow_copy_job_type(
    job_type: job_blocks.JobTypeVar) -> job_blocks.JobTypeVar:
  """Creates a shallow copy of the job structure."""

  def apply_to_job_group(job_group: job_blocks.JobGroup) -> job_blocks.JobGroup:
    job_group = copy.copy(job_group)
    job_group.jobs = {key: matcher(job) for key, job in job_group.jobs.items()}
    return job_group

  matcher = pattern_matching.match(
      pattern_matching.Case([job_blocks.Job], copy.copy),
      apply_to_job_group,
      pattern_matching.Case([job_blocks.JobGeneratorType],
                            lambda generator: generator),
  )
  return matcher(job_type)


def populate_job_names(job_type: job_blocks.JobTypeVar) -> None:
  """Assigns default names to the given jobs."""

  def apply_to_job(prefix: Sequence[str], target: job_blocks.Job) -> None:
    if target.name is None:
      target.name = '_'.join(prefix) if prefix else target.executable.name

  def apply_to_job_group(prefix: Sequence[str],
                         target: job_blocks.JobGroup) -> None:
    for key, job in target.jobs.items():
      matcher([*prefix, key], job)

  def ignore_unknown(_: Sequence[str], target: Any) -> None:
    return target

  matcher = pattern_matching.match(
      apply_to_job,
      apply_to_job_group,
      ignore_unknown,
  )
  return matcher([], job_type)


def collect_jobs_by_filter(
    job_group: job_blocks.JobGroup,
    predicate: Callable[[job_blocks.Job], bool],
) -> List[job_blocks.Job]:
  """Flattens a given job group and filters the result."""

  def match_job(job: job_blocks.Job) -> List[job_blocks.Job]:
    return [job] if predicate(job) else []

  def match_job_group(job_group: job_blocks.JobGroup) -> List[job_blocks.Job]:
    return list(
        itertools.chain.from_iterable(
            [job_collector(job) for job in job_group.jobs.values()]))

  job_collector = pattern_matching.match(match_job_group, match_job)
  return job_collector(job_group)


@attr.s(auto_attribs=True)
class ConstraintClique:
  """A constraint with the list of jobs it applies to."""

  constraint: job_blocks.Constraint
  jobs: List[job_blocks.Job]


def aggregate_constraint_cliques(
    job_group: job_blocks.JobGroup) -> List[ConstraintClique]:
  """Forms constraint cliques.

  For each constraint met, collects all jobs it applies to.

  Args:
    job_group: A job group to aggregate on.

  Returns:
    A set of cliques.
  """

  def match_job(
      job: job_blocks.Job
  ) -> Tuple[List[ConstraintClique], List[job_blocks.Job]]:
    return [], [job]

  def match_job_group(
      job_group: job_blocks.JobGroup
  ) -> Tuple[List[ConstraintClique], List[job_blocks.Job]]:
    cliques: List[ConstraintClique] = []
    jobs: List[job_blocks.Job] = []
    for job in job_group.jobs.values():
      subcliques, subjobs = matcher(job)
      cliques += subcliques
      jobs += subjobs
    cliques = [
        ConstraintClique(constraint, jobs)
        for constraint in job_group.constraints
    ] + cliques
    return cliques, jobs

  matcher = pattern_matching.match(match_job, match_job_group)
  result, _ = matcher(job_group)
  return result


def flatten_jobs(job_group: job_blocks.JobGroup) -> List[job_blocks.Job]:
  return collect_jobs_by_filter(job_group, lambda _: True)
