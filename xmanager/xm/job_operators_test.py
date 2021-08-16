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
from xmanager.xm import job_operators
from xmanager.xm import testing


def construct_job(name=None):
  return job_blocks.Job(
      name=name,
      executable=testing.TestExecutable(),
      executor=testing.TestExecutor())


class JobOperatorsTest(unittest.TestCase):

  def test_collect_jobs_by_filter_includes_matches(self):
    job_group = job_blocks.JobGroup(
        foo=construct_job('foo'),
        bar=construct_job('bar'),
        baz=construct_job('baz'),
    )

    self.assertEqual(
        job_operators.collect_jobs_by_filter(
            job_group,
            predicate=lambda job: job.name in ['foo', 'baz'],
        ),
        [job_group.jobs['foo'], job_group.jobs['baz']],
    )

  def test_collect_jobs_by_filter_handles_nested_groups(self):
    baz = construct_job('baz')
    foo = construct_job('foo')
    job_group = job_blocks.JobGroup(
        foo=foo,
        bar=job_blocks.JobGroup(baz=baz),
    )

    self.assertEqual(
        job_operators.collect_jobs_by_filter(
            job_group, predicate=lambda _: True),
        [foo, baz],
    )


if __name__ == '__main__':
  unittest.main()
