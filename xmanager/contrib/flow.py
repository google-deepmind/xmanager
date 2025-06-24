"""XMFlow-like feature."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, Callable, Optional

from absl import logging
from xmanager import xm
from xmanager.contrib import parameter_controller

_Fn = Callable[[xm.Experiment], Awaitable[None]]
_AsyncFn = Callable[[], xm.JobGeneratorType]
# The type of `parameter_controller.controller()`
_Controller = Callable[[_Fn], _AsyncFn]


class _UnlaunchedJobError(Exception):
  pass


class _UnlaunchedJob:

  async def wait_until_complete(self):
    raise _UnlaunchedJobError


class StopControllerError(Exception):
  pass


def executable_graph(
    *,
    jobs: dict[str, xm.JobType],
    jobs_deps: dict[str, Sequence[str]],
    jobs_args: dict[str, Mapping[str, Any] | None] | None = None,
    # Have to redefine external symbol to allow both
    # `flow.controller` and `flow.executable_graph(controller=)`
    controller: Optional[_Controller] = None,
    terminate_on_failure: bool = True,
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
    jobs_args: Mapping job-name to list of job arguments.
    controller: A `flow.controller()` (alias of
      `xmanager.contrib.parameter_controller.controller()`) to customize the
      executor parameters. If missing, a default executor is used.
    terminate_on_failure: If true, terminate upon the the first failure. If
      false, continue to launch jobs whose dependencies are successful.

  Returns:
    The controller to pass to `experiment.add()`
  """
  if jobs_args is None:
    jobs_args = {}

  # Normalize the graph by adding missing values
  for job_name in jobs:
    jobs_deps.setdefault(job_name, [])

  _assert_valid_graph(jobs_deps=jobs_deps, jobs=jobs)

  log(f'jobs: {list(jobs)}')
  log(f'jobs_deps: {jobs_deps}')

  controller = controller or parameter_controller.controller(
  )

  @controller
  async def run_graphs(experiment: xm.Experiment) -> None:

    jobs_launched = {job_name: asyncio.Future() for job_name in jobs}

    async def job_finished(job_name: str) -> bool:
      log(f'`Waiting for {job_name}` to be added to `experiment.add`')
      op = await jobs_launched[job_name]  # Wait for the `experiment.add`
      try:
        log(f'`{job_name}` is running, waiting to finish')
        await op.wait_until_complete()  # Wait for the job to complete
      except (xm.ExperimentUnitError, _UnlaunchedJobError) as e:
        log(f'`{job_name}` has failed.')
        if terminate_on_failure:
          raise StopControllerError() from e
        return False
      else:
        log(f'`{job_name}` has finished successfully.')
        return True

    async def launch_single_job(job_name: str) -> None:
      log(f'`Launching: {job_name}`, waiting for all the deps to schedule.')

      # Wait for all the deps to complete
      deps_finished = await asyncio.gather(
          *(job_finished(dep) for dep in jobs_deps[job_name])
      )
      # Schedule the job
      log(f'`All deps finished for: {job_name}`. Launching...')
      if all(deps_finished):
        op = await experiment.add(
            jobs[job_name], identity=job_name, args=jobs_args.get(job_name)
        )
      else:
        op = _UnlaunchedJob()
      log(f'`{job_name}` launched. Notify other waiting jobs...')
      # Notify other waiting jobs
      jobs_launched[job_name].set_result(op)
      log(f'`{job_name}` complete.')

    try:
      await asyncio.gather(*(launch_single_job(job_name) for job_name in jobs))
    except StopControllerError as e:
      log(str(e))
      # This is expected, so exit normally.
      return

  return run_graphs()  # pylint: disable=no-value-for-parameter


def _quote_name(x: str) -> str:
  """Return a quoted name for a node in a graphviz graph."""
  return '"' + x.replace('"', '\\"') + '"'


def _make_dot_graph_url(jobs_deps: dict[str, Sequence[str]]) -> str:
  # First add all leaf (potential singleton)
  terms = [_quote_name(j) for j, deps in jobs_deps.items() if not deps]
  for job_name, job_deps in jobs_deps.items():
    for dep in job_deps:
      terms.append(f'{_quote_name(dep)}->{_quote_name(job_name)}')
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


def log(msg: str) -> None:
  """Log messages."""
  logging.info(msg)
