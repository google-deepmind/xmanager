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
from xmanager.xm import testing
from xmanager.xm import utils


async def make_me_a_sandwich() -> str:
  return 'sandwich'


def construct_job(name=None):
  return job_blocks.Job(
      name=name,
      executable=testing.TestExecutable(),
      executor=testing.TestExecutor())


class UtilsTest(unittest.TestCase):

  def test_to_command_line_args_dict(self):
    self.assertEqual(
        utils.to_command_line_args({
            'foo': 1,
            'bar': [2, 3]
        }), ['--foo', '1', '--bar', "'[2, 3]'"])

  def test_to_command_line_args_list(self):
    self.assertEqual(
        utils.to_command_line_args(['--foo', 12.0, (1, 2, 3)]),
        ['--foo', '12.0', "'(1, 2, 3)'"])

  def test_to_command_line_args_no_escape(self):
    self.assertEqual(
        utils.to_command_line_args([utils.ShellSafeArg('$TMP')]), ['$TMP'])

  @utils.run_in_asyncio_loop
  async def test_run_in_asyncio_loop(self):
    self.assertEqual(await make_me_a_sandwich(), 'sandwich')

  def test_run_in_asyncio_loop_returns_value(self):
    self.assertEqual(
        utils.run_in_asyncio_loop(make_me_a_sandwich)(), 'sandwich')

  def test_collect_jobs_by_filter(self):
    job_group = job_blocks.JobGroup(
        foo=construct_job('foo'),
        bar=construct_job('bar'),
        baz=construct_job('baz'),
    )

    self.assertEqual(
        utils.collect_jobs_by_filter(
            job_group,
            predicate=lambda job: job.name in ['foo', 'baz'],
        ),
        [job_group.jobs['foo'], job_group.jobs['baz']],
    )

  def test_collect_jobs_by_filter_nested(self):
    baz = construct_job('baz')
    foo = construct_job('foo')
    job_group = job_blocks.JobGroup(
        foo=foo,
        bar=job_blocks.JobGroup(baz=baz),
    )

    self.assertEqual(
        utils.collect_jobs_by_filter(job_group, predicate=lambda _: True),
        [foo, baz],
    )


if __name__ == '__main__':
  unittest.main()
