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

from xmanager.xm import core
from xmanager.xm import testing
from xmanager.xm import utils


async def make_me_a_sandwich() -> str:
  return 'sandwich'


def construct_job(args):
  return core.Job(
      executable=core.Executable(), executor=testing.TestExecutor(), args=args)


class UtilsTest(unittest.TestCase):

  def test_to_command_line_args_dict(self):
    self.assertEqual(
        utils.to_command_line_args({
            'foo': 1,
            'bar': [2, 3]
        }), ['--foo=1', "--bar='[2, 3]'"])

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
    job_group = core.JobGroup(
        foo=construct_job(args=['foo']),
        bar=construct_job(args=['bar']),
        baz=construct_job(args=['baz']),
    )
    self.assertCountEqual(
        utils.collect_jobs_by_filter(
            job_group, predicate=lambda job: 'bar' in job.args),
        (job_group.jobs['bar'],),
    )
    self.assertEqual(
        utils.collect_jobs_by_filter_with_names(
            job_group,
            predicate=lambda name, job: 'baz' in job.args or name == 'foo',
        ),
        {
            'foo': job_group.jobs['foo'],
            'baz': job_group.jobs['baz'],
        },
    )

  def test_collect_jobs_by_filter_nested(self):
    baz = construct_job(args=['baz'])
    foo = construct_job(args=['foo'])
    job_group = core.JobGroup(
        foo=foo,
        bar=core.JobGroup(baz=baz),
    )
    self.assertCountEqual(
        utils.collect_jobs_by_filter(job_group, predicate=lambda _: True),
        (foo, baz),
    )
    self.assertEqual(
        utils.collect_jobs_by_filter_with_names(
            job_group, predicate=lambda _, __: True),
        {
            'foo': foo,
            'bar.baz': baz,
        },
    )


if __name__ == '__main__':
  unittest.main()
