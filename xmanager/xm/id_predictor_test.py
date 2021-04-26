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

import asyncio
import unittest

from xmanager.xm import id_predictor
from xmanager.xm import utils


class IdPredictorTest(unittest.TestCase):

  @utils.run_in_asyncio_loop
  async def test_first_id_is_correct(self):
    """Simple Predictor usage example."""
    predictor = id_predictor.Predictor(next_id=1)

    first_id = predictor.reserve_id()
    async with predictor.submit_id(first_id):
      self.assertEqual(first_id, 1)

  @utils.run_in_asyncio_loop
  async def test_ids_are_submitted_in_order(self):
    predictor = id_predictor.Predictor(next_id=1)

    self.assertEqual(predictor.reserve_id(), 1)
    self.assertEqual(predictor.reserve_id(), 2)
    self.assertEqual(predictor.reserve_id(), 3)

    submitted_ids = []

    async def submit(id_to_submit):
      async with predictor.submit_id(id_to_submit):
        submitted_ids.append(id_to_submit)

    await asyncio.gather(submit(3), submit(2), submit(1))

    self.assertEqual(submitted_ids, [1, 2, 3])

  @utils.run_in_asyncio_loop
  async def test_broken_sequence(self):
    predictor = id_predictor.Predictor(next_id=1)

    self.assertEqual(predictor.reserve_id(), 1)
    self.assertEqual(predictor.reserve_id(), 2)

    with self.assertRaises(RuntimeError):
      async with predictor.submit_id(1):
        raise RuntimeError('Id was eaten by a giant space ant.')

    with self.assertRaises(id_predictor.BrokenSequenceError):
      async with predictor.submit_id(2):
        pass


if __name__ == '__main__':
  unittest.main()
