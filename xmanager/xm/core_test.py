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
import threading
import unittest

from xmanager.xm import core
from xmanager.xm import job_blocks
from xmanager.xm import testing
from xmanager.xm import utils


class TestError(RuntimeError):
  """Exception which can be used in tests below."""


async def failing_job_generator(work_unit: core.WorkUnit):
  raise TestError


class ExperimentTest(unittest.TestCase):

  def test_single_job_launch(self):
    experiment = testing.TestExperiment()
    with experiment:
      job = job_blocks.Job(
          testing.TestExecutable(),
          testing.TestExecutor(),
          args={},
          name='name')
      experiment.add(job)

    self.assertEqual(experiment.launched_jobs, [job])

  def test_job_group_launch(self):
    experiment = testing.TestExperiment()
    with experiment:
      foo_job = job_blocks.Job(
          testing.TestExecutable(),
          testing.TestExecutor(),
          args={'foo': 1},
          name='1')
      bar_job = job_blocks.Job(
          testing.TestExecutable(),
          testing.TestExecutor(),
          args={'bar': 2},
          name='2')
      experiment.add(job_blocks.JobGroup(foo=foo_job, bar=bar_job))

    self.assertEqual(experiment.launched_jobs, [foo_job, bar_job])

  def test_job_generator_launch(self):
    experiment = testing.TestExperiment()
    with experiment:
      job = job_blocks.Job(
          testing.TestExecutable(),
          testing.TestExecutor(),
          args={},
          name='name')

      async def job_generator(work_unit: core.WorkUnit, use_magic: bool):
        self.assertEqual(use_magic, True)
        work_unit.add(job)

      experiment.add(job_generator, args={'use_magic': True})

    self.assertEqual(experiment.launched_jobs, [job])
    self.assertEqual(experiment.launched_jobs_args, [{'use_magic': True}])

  def test_job_generator_raises(self):
    experiment = testing.TestExperiment()
    with self.assertRaises(TestError):
      with experiment:
        experiment.add(failing_job_generator)

  def test_non_async_job_generator_raises_user_friendly_exception(self):
    with self.assertRaisesRegex(ValueError, '.* generator must be an async .*'):
      with testing.TestExperiment() as experiment:

        def job_generator(work_unit: core.WorkUnit):
          del work_unit

        experiment.add(job_generator)

  def test_launch_with_args(self):
    experiment = testing.TestExperiment()
    with experiment:
      experiment.add(
          job_blocks.JobGroup(
              foo=job_blocks.Job(
                  testing.TestExecutable(),
                  testing.TestExecutor(),
                  args={
                      'x': 1,
                      'y': 2
                  },
                  env_vars={'EDITOR': 'vi'}),
              bar=job_blocks.Job(
                  testing.TestExecutable(),
                  testing.TestExecutor(),
                  args=['--bar=1'])),
          args={
              'foo': {
                  'args': {
                      'x': 3,
                      'z': 4
                  },
                  'env_vars': {
                      'TURBO': 'ON'
                  }
              },
              'bar': {
                  'args': ['--spacebar']
              },
          })

    self.assertEqual(
        experiment.launched_jobs[0].args,
        job_blocks.SequentialArgs.from_collection({
            'x': 3,
            'y': 2,
            'z': 4
        }),
    )
    self.assertEqual(experiment.launched_jobs[0].env_vars, {
        'TURBO': 'ON',
        'EDITOR': 'vi'
    })
    self.assertEqual(
        experiment.launched_jobs[1].args,
        job_blocks.SequentialArgs.from_collection(['--bar=1', '--spacebar']),
    )

  def test_add_runs_asynchronously(self):
    generator_called = threading.Event()

    with testing.TestExperiment() as experiment:

      async def job_generator(work_unit: core.WorkUnit):
        del work_unit
        generator_called.set()

      experiment.add(job_generator)

      # Validate that job_generator is executed in a parallel thread.
      self.assertTrue(generator_called.wait(timeout=5))

  @utils.run_in_asyncio_loop
  async def test_loop_is_reused_in_coro_context(self):
    loop = asyncio.get_event_loop()

    async with testing.TestExperiment() as experiment:

      async def job_generator(work_unit: core.WorkUnit):
        del work_unit
        self.assertEqual(asyncio.get_event_loop(), loop)

      experiment.add(job_generator)

  @utils.run_in_asyncio_loop
  async def test_sync_with_cant_be_used_in_coro_context(self):
    # `async with` works.
    async with testing.TestExperiment():
      pass

    with self.assertRaises(RuntimeError):
      # But `with` raises an exception.
      with testing.TestExperiment():
        pass

  @utils.run_in_asyncio_loop
  async def test_work_unit_wait_until_complete(self):
    experiment = testing.TestExperiment()
    async with experiment:
      experiment.add(
          job_blocks.Job(
              testing.TestExecutable(), testing.TestExecutor(), args={}))
      await experiment.work_units[0].wait_until_complete()

  @utils.run_in_asyncio_loop
  async def test_work_unit_wait_until_complete_exception(self):
    experiment = testing.TestExperiment()
    with self.assertRaises(TestError):
      async with experiment:
        experiment.add(failing_job_generator)
        with self.assertRaises(core.ExperimentUnitError):
          await experiment.work_units[0].wait_until_complete()

  @utils.run_in_asyncio_loop
  async def test_get_full_job_name(self):

    async def generator(work_unit):
      self.assertEqual(work_unit.get_full_job_name('name'), '1_1_name')

    async with testing.TestExperiment() as experiment:
      experiment.add(generator)


if __name__ == '__main__':
  unittest.main()
