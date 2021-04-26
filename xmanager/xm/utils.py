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
"""Utility functions needed for XManager API implementation.

This module is private and can only be used by the API itself, but not by users.
"""

import asyncio
import functools
import itertools
import shlex
from typing import Any, Callable, Dict, List, Mapping, TypeVar

import attr

from xmanager.xm import core
from xmanager.xm import pattern_matching

ReturnType = TypeVar('ReturnType')


@attr.s(auto_attribs=True)
class ShellSafeArg:
  """Command line argument that shouldn't be escaped.

  Normally all arguments would be passed to the binary as is. To let shell
  substitutions, such as environment variable expansion, to happen the argument
  must be wrapped with this structure.
  """
  arg: str


def _escape(arg: Any) -> str:
  """Returns a shell-safe text representation of the argument."""
  if isinstance(arg, ShellSafeArg):
    return arg.arg
  else:
    return shlex.quote(str(arg))


def to_command_line_args(args: core.ArgsType) -> List[str]:
  """Returns arguments representation suitable to passing to a binary.

  For user convenience we allow arguments to be passed as dicts and don't impose
  strong typing requirements. But at the end of a day command line arguments
  need to become just a list of strings.

  Args:
    args: command line arguments for an executable.
  """
  if isinstance(args, Mapping):
    return [f'--{k}={_escape(v)}' for k, v in args.items()]
  else:
    return [_escape(arg) for arg in args]


def run_in_asyncio_loop(
    f: Callable[..., ReturnType]) -> Callable[..., ReturnType]:
  """A decorator that turns an async function to a synchronous one.

  Python asynchronous APIs can't be used directly from synchronous functions.
  While wrapping them with an asyncio loop requires little code, in some
  contexts it results in too much boilerplate.

  Testing async functions:

    class MyTest(unittest.TestCase):
      @run_in_asyncio_loop
      async def test_my_async_function(self):
        self.assertEqual(await async_function(), 42)

  Running the whole program in an event loop:

    @run_in_asyncio_loop
    async def main(argv):
      print('Hello world')

    if __name__ == '__main__':
      app.run(main)

  It is not advised to use this decorator beyond these two cases.

  Args:
    f: An async function to run in a loop.

  Returns:
    A synchronous function with the same arguments.
  """

  @functools.wraps(f)
  def decorated(*args, **kwargs) -> ReturnType:
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(f(*args, **kwargs))

  return decorated


def collect_jobs_by_filter(
    job_group: core.JobGroup,
    predicate: Callable[[core.Job], bool],
) -> List[core.Job]:
  """Flattens a given job group and filters the result."""
  return list(
      collect_jobs_by_filter_with_names(
          job_group,
          predicate=lambda _, job: predicate(job),
      ).values())


def collect_jobs_by_filter_with_names(
    job_group: core.JobGroup,
    predicate: Callable[[str, core.Job], bool],
) -> Dict[str, core.Job]:
  """Flattens a given job group and filters the result, preserving job names."""

  def match_job(name: str, job: core.Job) -> Dict[str, core.Job]:
    return {name: job} if predicate(name, job) else {}

  def match_job_group_with_name(
      job_group_name: str, job_group: core.JobGroup) -> Dict[str, core.Job]:
    flattened_group = itertools.chain.from_iterable(
        job_collector(name, job).items()
        for name, job in job_group.jobs.items())
    return {f'{job_group_name}.{name}': job for name, job in flattened_group}

  def match_job_group(job_group: core.JobGroup) -> Dict[str, core.Job]:
    return dict(
        itertools.chain.from_iterable(
            job_collector(name, job).items()
            for name, job in job_group.jobs.items()))

  job_collector = pattern_matching.match(match_job_group,
                                         match_job_group_with_name, match_job)
  return job_collector(job_group)
