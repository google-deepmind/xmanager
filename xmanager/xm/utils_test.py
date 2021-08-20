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

import unittest

from xmanager.xm import utils


async def make_me_a_sandwich() -> str:
  return 'sandwich'


class UtilsTest(unittest.TestCase):

  @utils.run_in_asyncio_loop
  async def test_run_in_asyncio_loop(self):
    self.assertEqual(await make_me_a_sandwich(), 'sandwich')

  def test_run_in_asyncio_loop_returns_value(self):
    self.assertEqual(
        utils.run_in_asyncio_loop(make_me_a_sandwich)(), 'sandwich')


if __name__ == '__main__':
  unittest.main()
