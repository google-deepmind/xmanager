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
import concurrent.futures
import enum
import functools
import os
import shlex
import sys
from typing import Any, Awaitable, Callable, TypeVar

from absl import flags
import attr

FLAGS = flags.FLAGS
flags.DEFINE_string(
   'xm_launch_script', None, 'Path to the launch script that is using '
   'XManager Launch API')

ReturnT = TypeVar('ReturnT')


class SpecialArg(abc.ABC):
  """A base class for arguments with special handling on serialization."""


@attr.s(auto_attribs=True)
class ShellSafeArg(SpecialArg):
  """Command line argument that shouldn't be quoted.

  Backends that run the job through a shell quote every argument, which
  suppresses substitutions such as environment variable expansion. Wrapping an
  argument in this structure exempts it from that quoting. Backends that hand
  arguments over as `argv` neither quote nor substitute, so there the wrapper
  makes no difference.
  """

  arg: str

  def __str__(self) -> str:
    """Prevents ShellSafeArg from being used in f-strings."""
    raise RuntimeError(
        f'Converting {self!r} to a string would strip the ShellSafe semantics.'
    )


def ARGV_ESCAPER(value: Any) -> str:  # pylint: disable=invalid-name
  """Serializes a value into an `argv` token for a backend with no shell."""
  match value:
    case ShellSafeArg():
      return value.arg
    case enum.Enum():
      return str(value.name)
    case _:
      return str(value)


def ARG_ESCAPER(value: Any) -> str:  # pylint: disable=invalid-name
  """Serializes a value into a token of a command destined for a POSIX shell."""
  match value:
    case ShellSafeArg():
      return value.arg
    case _:
      return shlex.quote(ARGV_ESCAPER(value))


def trivial_kwargs_joiner(key: str, value: str) -> str:
  """Concatenates keyword arguments with = sign."""
  return f'{key}={value}'


def _is_in_running_loop() -> bool:
  try:
    asyncio.get_running_loop()
    return True
  except RuntimeError:
    return False


def run_in_asyncio_loop(
    f: Callable[..., Awaitable[ReturnT]],
) -> Callable[..., ReturnT]:
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
  def decorated(*args, **kwargs) -> ReturnT:

    def _run() -> ReturnT:
      loop = asyncio.new_event_loop()
      try:
        return loop.run_until_complete(f(*args, **kwargs))
      finally:
        loop.close()

    if _is_in_running_loop():
      # Nested event loops are not allowed, so run in a new thread.
      with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(_run).result()
    else:
      return _run()

  return decorated


@functools.lru_cache()
def find_launch_script_path() -> str:
  """Finds the launch script path."""
  # We can get the launch script if it's provided explicitly, or when it's run
  # using a Python interpreter.
  launch_script_path = sys.argv[0]
  if hasattr(FLAGS, 'xm_launch_script') and FLAGS.xm_launch_script:
    launch_script_path = FLAGS.xm_launch_script
  if not launch_script_path.endswith('.py'):
    # If the launch script is built with subpar we are interested in the name
    # of the main module, rather than subpar binary.
    main_file_path = getattr(sys.modules['__main__'], '__file__', None)
    if main_file_path and os.access(main_file_path, os.R_OK):
      launch_script_path = main_file_path

  if not launch_script_path:
    return ''

  # The path may be relative, especially if it comes from sys.argv[0].
  return os.path.abspath(launch_script_path)


def resolve_path_relative_to_launcher(path: str) -> str:
  """Get the absolute assuming paths are relative to the launcher script file.

  Using this method a launcher script can refer to its own directory or parent
  directory via `.` and `..`.

  Args:
    path: Path that may be relative to the launch script.

  Returns:
    Absolute path.

  Raises:
    RuntimeError: If unable to determine the launch script path.
  """
  if os.path.isabs(path):
    return path

  launch_script_path = find_launch_script_path()
  if not os.access(launch_script_path, os.R_OK):
    raise RuntimeError(
        'Unable to determine launch script path. '
        f'The script is not present at {launch_script_path!r}. '
        'This may happen if launch script changes the '
        'working directory.'
    )
  caller_file_path = os.path.realpath(launch_script_path)
  caller_dir = os.path.dirname(caller_file_path)
  return os.path.realpath(os.path.join(caller_dir, path))
