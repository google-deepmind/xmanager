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

import abc
import asyncio
import functools
import inspect
import os
import shlex
from typing import Any, Callable, TypeVar

import attr

from xmanager.xm import pattern_matching

ReturnType = TypeVar('ReturnType')


class SpecialArg(abc.ABC):
  """A base class for arguments with special handling on serialization."""


@attr.s(auto_attribs=True)
class ShellSafeArg(SpecialArg):
  """Command line argument that shouldn't be escaped.

  Normally all arguments would be passed to the binary as is. To let shell
  substitutions (such as environment variable expansion) to happen the argument
  must be wrapped with this structure.
  """

  arg: str


ARG_ESCAPER = pattern_matching.match(
    pattern_matching.Case([ShellSafeArg], lambda v: v.arg),
    pattern_matching.Case([Any], lambda v: shlex.quote(str(v))),
)


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
    asyncio.get_child_watcher().attach_loop(loop)
    return loop.run_until_complete(f(*args, **kwargs))

  return decorated


def get_absolute_path(path: str) -> str:
  """Gets the abspath when relative paths are used in the launcher script.

  A launcher script can refer to its own directory or parent directory via
  `.` and `..`.

  Args:
    path: Path that may contain relative paths relative to the launcher script.

  Returns:
    Absolute path.
  """
  if os.path.isabs(path):
    return path

  # WARNING: This line assumes that the call stack looks like:
  # [0] utils.py
  # [1] executables.py
  # [2] launcher.py
  caller_filename = os.path.realpath(inspect.stack()[2].filename)
  caller_dir = os.path.dirname(caller_filename)
  return os.path.realpath(os.path.join(caller_dir, path))
