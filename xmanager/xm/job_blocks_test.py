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

from xmanager.xm import job_blocks


class JobBlocksTest(unittest.TestCase):

  def test_from_mapping(self):
    args = job_blocks.SequentialArgs.from_collection({
        'a': 1,
        'b': 2,
        'c': 3,
    })

    self.assertEqual(args.to_list(str), ['--a', '1', '--b', '2', '--c', '3'])

  def test_from_sequence(self):
    args = job_blocks.SequentialArgs.from_collection([1, 2, 3])

    self.assertEqual(args.to_list(str), ['1', '2', '3'])

  def test_merge(self):
    args = job_blocks.SequentialArgs.merge(
        map(job_blocks.SequentialArgs.from_collection, [[1], {
            'a': 'z',
            'b': 'y'
        }, [2], {
            'b': 'x',
            'c': 't'
        }, [3]]))

    self.assertEqual(
        args.to_list(str), ['1', '--a', 'z', '--b', 'x', '2', '--c', 't', '3'])

  def test_to_dict(self):
    args = job_blocks.SequentialArgs.merge(
        map(job_blocks.SequentialArgs.from_collection, [['--knob'], {
            1: False
        }]))

    self.assertEqual(args.to_dict(), {'--knob': True, '1': False})


if __name__ == '__main__':
  unittest.main()
