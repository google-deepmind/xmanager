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

import pickle
from typing import Sequence
import unittest

from xmanager.xm import async_packager
from xmanager.xm import job_blocks
from xmanager.xm import utils


def _package_batch(
    packageables: Sequence[job_blocks.Packageable],
) -> Sequence[job_blocks.Executable]:
  return [
      job_blocks.Executable(name=packageable.executable_spec.name)
      for packageable in packageables
  ]


class _TestExecutableSpec(job_blocks.ExecutableSpec):

  def __init__(self, name: str) -> None:
    self._name = name

  @property
  def name(self) -> str:
    return self._name


def _make_packageable(name: str) -> job_blocks.Packageable:
  return job_blocks.Packageable(
      executable_spec=_TestExecutableSpec(name),
      executor_spec=job_blocks.ExecutorSpec(),
  )


class AsyncPackagerTest(unittest.TestCase):

  @utils.run_in_asyncio_loop
  async def test_async_packager_end_to_end(self):
    packager = async_packager.AsyncPackager(_package_batch)
    executable1 = packager.add(_make_packageable('1'))
    executable2 = packager.add(_make_packageable('2'))
    packager.package()
    self.assertEqual((await executable1).name, '1')
    self.assertEqual((await executable2).name, '2')

  @utils.run_in_asyncio_loop
  async def test_package_with_extra_packageables(self):
    packager = async_packager.AsyncPackager(_package_batch)
    async_executable = packager.add(_make_packageable('async'))
    [extra_executable] = packager.package((_make_packageable('extra'),))
    self.assertEqual((await async_executable).name, 'async')
    self.assertEqual(extra_executable.name, 'extra')

  @utils.run_in_asyncio_loop
  async def test_package_is_required(self):
    packager = async_packager.AsyncPackager(_package_batch)
    executable = packager.add(_make_packageable(''))
    with self.assertRaises(async_packager.PackageHasNotBeenCalledError):
      await executable

  def test_awaitable_is_picklable(self):
    packager = async_packager.AsyncPackager(_package_batch)
    executable = packager.add(_make_packageable(''))
    packager.package()
    executable_str = pickle.dumps(executable)

    # Wait for the executable in a separate event loop, which did not even exist
    # when we requested packaging.
    @utils.run_in_asyncio_loop
    async def wait_for_it():
      await pickle.loads(executable_str)

    wait_for_it()


if __name__ == '__main__':
  unittest.main()
