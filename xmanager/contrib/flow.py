# pyformat: mode=pyink
"""XMFlow-like feature."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Callable, Optional

from absl import logging
from xmanager import xm
from xmanager.contrib import parameter_controller

_Fn = Callable[[xm.Experiment], None]
_AsyncFn = Callable[[], xm.JobGeneratorType]
# A `flow.controller()` (alias of `parameter_controller.controller()`)
_Controller = Callable[[_Fn], _AsyncFn]


# Alias for convenience.
controller = parameter_controller.controller


def executable_graph(
    *,
    jobs: dict[str, xm.JobType],
    jobs_deps: dict[str, Sequence[str]],
    # Have to redefine external symbol to allow both
    # `flow.controller` and `flow.executable_graph(controller=)`
    controller: Optional[_Controller] = None,  # pylint: disable=redefined-outer-name
) -> xm.JobGeneratorType:
  """Returns an executable which run the pipeline.

  Usage:

  ```python
  from xmanager.contrib import flow

  experiment.add(flow.executable_graph(
      jobs={
          'preprocessing': xm.Job(),
          'train': xm.Job(),
          'eval': xm.Job(),
          'final': xm.Job(),
      },
      jobs_deps={
          'train': ['preprocessing'],
          'eval': ['preprocessing'],
          'final': ['train', 'eval'],
      },
  ))
  ```

  Args:
    jobs: Jobs to run (in order defined by the `jobd_deps` graph). Jobs not
      defined in `jobs_deps` are assumed to not have any dependencies and run
      directly.
    jobs_deps: Mapping job-name to list of job dependencies.
    controller: A `flow.controller()` (alias of
      `xmanager.contrib.parameter_controller.controller()`) to customize the
      executor parameters. If missing, a default executor is used.

  Returns:
    The controller to pass to `experiment.add()`
  """
  # Normalize the graph by adding missing values
  for job_name in jobs:
    jobs_deps.setdefault(job_name, [])

  _assert_valid_graph(jobs_deps=jobs_deps, jobs=jobs)

  controller = controller or parameter_controller.controller(
  )

  @controller
  async def run_graphs(experiment: xm.Experiment) -> None:

    jobs_launched = {job_name: asyncio.Future() for job_name in jobs}

    async def job_finished(job_name):
      op = await jobs_launched[job_name]  # Wait for the `experiment.add`
      await op.wait_until_complete()  # Wait for the job to complete

    async def launch_single_job(job_name):
      # Wait for all the deps to complete
      await asyncio.gather(*(job_finished(dep) for dep in jobs_deps[job_name]))
      # Schedule the job
      op = await experiment.add(jobs[job_name], identity=job_name)
      # Notify other waiting jobs
      jobs_launched[job_name].set_result(op)

    await asyncio.gather(*(launch_single_job(job_name) for job_name in jobs))

  return run_graphs()  # pylint: disable=no-value-for-parameter


def _normalize_name(x: str) -> str:
  """Replace whitespace with underscores."""
  return x.replace(' ', '_')


def _make_dot_graph_url(jobs_deps: dict[str, Sequence[str]]) -> str:
  # First add all leaf (potential singleton)
  terms = [_normalize_name(j) for j, deps in jobs_deps.items() if not deps]
  for job_name, job_deps in jobs_deps.items():
    for dep in job_deps:
      terms.append(f'{_normalize_name(job_name)}->{_normalize_name(dep)}')
  dot = 'digraph{{{}}}'.format(' '.join(terms))
  return dot


def _assert_valid_graph(
    *,
    jobs: dict[str, xm.Job],
    jobs_deps: dict[str, Sequence[str]],
):
  """Validate the jobs are valid."""
  all_job_names = set()
  for j, jd in jobs_deps.items():
    all_job_names.add(j)
    all_job_names.update(jd)

  if extra_jobs := sorted(all_job_names - set(jobs)):
    raise ValueError(
        'Invalid `jobs_deps`: Some dependencies are not present in the '
        f'`jobs=`: {extra_jobs}'
    )
  # Could also detect cycles, but likely over-engineered
