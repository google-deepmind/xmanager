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
from unittest import mock

from absl.testing import absltest
from xmanager.xm import job_blocks


class JobBlocksTest(unittest.TestCase):

  def test_from_mapping(self):
    args = job_blocks.SequentialArgs.from_collection({
        'a': 1,
        'b': 2,
        'c': 3,
    })

    self.assertEqual(args.to_list(str), ['--a=1', '--b=2', '--c=3'])

  def test_from_mapping_multi(self):
    args = job_blocks.SequentialArgs.from_collection({
        'a': 1,
        'c': [3, '4'],
    })

    self.assertEqual(args.to_list(str), ['--a=1', '--c=3', '--c=4'])

  def test_from_mapping_nested_multi(self):
    args = job_blocks.SequentialArgs.from_collection({'a': [[1, 2, 3]]})

    self.assertEqual(args.to_list(str), ['--a=[[1, 2, 3]]'])

  def test_from_sequence(self):
    args = job_blocks.SequentialArgs.from_collection([1, 2, 3])

    self.assertEqual(args.to_list(str), ['1', '2', '3'])

  def test_from_none(self):
    args = job_blocks.SequentialArgs.from_collection(None)
    self.assertEqual(args.to_list(str), [])

  def test_merge_args(self):
    args = job_blocks.merge_args(
        [1],
        {
            'a': 'z',
            'b': 'y',
        },
        [2],
        {
            'b': 'x',
            'c': 't',
        },
        [3],
    )

    self.assertEqual(
        args.to_list(str), ['1', '--a=z', '--b=x', '2', '--c=t', '3']
    )

  def test_to_dict(self):
    args = job_blocks.merge_args(['--knob'], {1: False})

    self.assertEqual(args.to_dict(), {'--knob': True, '1': False})

  def test_to_list_bool(self):
    args = job_blocks.SequentialArgs.from_collection({'yes': True, 'no': False})

    self.assertEqual(args.to_list(str), ['--yes', '--nono'])

  def test_to_list_none(self):
    args = job_blocks.SequentialArgs.from_collection(
        {'skip_me': None, 'pass_me': 'None'}
    )

    self.assertEqual(args.to_list(str), ['--pass_me=None'])

  def test_sequential_args_from_string(self):
    with self.assertRaisesRegex(
        ValueError,
        (
            "Tried to construct xm.SequentialArgs from a string: '--foo'. "
            "Wrap it in a list: \\['--foo'\\] to make it a single argument."
        ),
    ):
      job_blocks.SequentialArgs.from_collection('--foo')

  def test_get_args_for_all_jobs(self):
    group = job_blocks.JobGroup(
        a=job_blocks.Job(mock.Mock(), mock.Mock()),
        b=job_blocks.JobGroup(
            b1=job_blocks.JobGroup(
                b1i=job_blocks.Job(mock.Mock(), mock.Mock()),
                b1ii=job_blocks.Job(mock.Mock(), mock.Mock()),
            ),
            b2=job_blocks.Job(mock.Mock(), mock.Mock()),
        ),
    )
    logdir = {'logdir': '/logdir/1'}
    expected = {
        'a': {'args': logdir},
        'b': {
            'b1': {
                'b1i': {'args': logdir},
                'b1ii': {'args': logdir},
            },
            'b2': {'args': logdir},
        },
    }
    args = job_blocks.get_args_for_all_jobs(group, logdir)
    self.assertDictEqual(expected, args)


if __name__ == '__main__':
  absltest.main()
