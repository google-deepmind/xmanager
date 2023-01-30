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
"""Tests for xm_tensorflow."""

import json
from typing import cast

from absl.testing import absltest
from absl.testing import parameterized
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import addressing
from xmanager.contrib import xm_tensorflow


class XmTensorflowTest(parameterized.TestCase):

  @parameterized.product(num_workers=[0, 2, 4])
  def test_kubernetes_multiworker_strategy(self, num_workers):
    experiment = absltest.mock.MagicMock()
    type(experiment).experiment_id = absltest.mock.PropertyMock(
        return_value=123
    )
    work_unit = absltest.mock.MagicMock()
    type(work_unit).work_unit_id = absltest.mock.PropertyMock(return_value=42)
    executable = absltest.mock.MagicMock()
    executor = absltest.mock.MagicMock(spec=xm_local.Kubernetes)
    hparams = xm.SequentialArgs.from_collection({'a': 'b'})
    worker_name = 'best_worker'

    builder = xm_tensorflow.MultiWorkerMirroredStrategyBuilder(
        experiment=experiment,
        worker_executable=executable,
        worker_executor=executor,
        num_workers=num_workers,
        worker_name=worker_name,
    )
    job_group = builder.create_job_group(hparams=hparams, work_unit=work_unit)

    expected_job_names = [f'{worker_name}-{i}' for i in range(num_workers)]
    expected_worker_domains = [
        addressing.k8s_pod_domain(
            job_name=job_name, experiment_id=123, work_unit_id=42
        )
        for job_name in expected_job_names
    ]

    self.assertSameElements(expected_job_names, list(job_group.jobs.keys()))

    for i, job_name in enumerate(expected_job_names):
      self.assertIsInstance(job_group.jobs[job_name], xm.Job)
      job = cast(xm.Job, job_group.jobs[job_name])

      self.assertEqual(job.executable, executable)
      self.assertEqual(job.executor, executor)
      self.assertEqual(job.args, xm.SequentialArgs.from_collection(hparams))

      tf_config = {
          'cluster': {'worker': expected_worker_domains},
          'task': {'type': 'worker', 'index': i},
      }
      self.assertEqual(job.env_vars, {'TF_CONFIG': json.dumps(tf_config)})

  @parameterized.product(num_workers=[0, 2], num_ps=[0, 2])
  def test_kubernetes_parameter_server_strategy(self, num_workers, num_ps):
    experiment = absltest.mock.MagicMock()
    type(experiment).experiment_id = absltest.mock.PropertyMock(
        return_value=123
    )
    work_unit = absltest.mock.MagicMock()
    type(work_unit).work_unit_id = absltest.mock.PropertyMock(return_value=42)

    chief_executable = absltest.mock.MagicMock()
    chief_executor = absltest.mock.MagicMock(spec=xm_local.Kubernetes)
    worker_executable = absltest.mock.MagicMock()
    worker_executor = absltest.mock.MagicMock(spec=xm_local.Kubernetes)
    ps_executable = absltest.mock.MagicMock()
    ps_executor = absltest.mock.MagicMock(spec=xm_local.Kubernetes)
    chief_name = 'best_chief'
    worker_name = 'best_worker'
    ps_name = 'best_ps'

    hparams = xm.SequentialArgs.from_collection({'a': 'b'})

    builder = xm_tensorflow.ParameterServerStrategyBuilder(
        experiment,
        chief_executable,
        chief_executor,
        worker_executable,
        worker_executor,
        ps_executable,
        ps_executor,
        chief_name=chief_name,
        worker_name=worker_name,
        ps_name=ps_name,
        num_workers=num_workers,
        num_ps=num_ps,
    )
    job_group = builder.create_job_group(work_unit=work_unit, hparams=hparams)

    expected_worker_job_names = [
        f'{worker_name}-{i}' for i in range(num_workers)
    ]
    expected_ps_job_names = [f'{ps_name}-{i}' for i in range(num_ps)]
    self.assertSameElements(
        [chief_name] + expected_worker_job_names + expected_ps_job_names,
        list(job_group.jobs.keys()),
    )

    expected_chief_domain = addressing.k8s_pod_domain(
        chief_name, experiment_id=123, work_unit_id=42
    )
    expected_worker_domains = [
        addressing.k8s_pod_domain(worker_name, 123, 42)
        for worker_name in expected_worker_job_names
    ]
    expected_ps_domains = [
        addressing.k8s_pod_domain(ps_name, 123, 42)
        for ps_name in expected_ps_job_names
    ]

    for job in job_group.jobs.values():
      self.assertIsInstance(job, xm.Job)

    # Test chief
    job = cast(xm.Job, job_group.jobs[chief_name])

    def _create_expected_tf_config(task_type, task_index):
      return {
          'cluster': {
              'chief': [expected_chief_domain],
              'worker': expected_worker_domains,
              'ps': expected_ps_domains,
          },
          'task': {
              'type': task_type,
              'index': task_index,
          },
      }

    self.assertEqual(job.executable, chief_executable)
    self.assertEqual(job.executor, chief_executor)
    self.assertEqual(
        job.env_vars,
        {'TF_CONFIG': json.dumps(_create_expected_tf_config('chief', 0))},
    )

    # Test workers
    for i in range(num_workers):
      worker_job_name = expected_worker_job_names[i]
      job = cast(xm.Job, job_group.jobs[worker_job_name])

      self.assertEqual(job.executable, worker_executable)
      self.assertEqual(job.executor, worker_executor)
      self.assertEqual(job.args, xm.SequentialArgs.from_collection(hparams))
      self.assertEqual(
          job.env_vars,
          {'TF_CONFIG': json.dumps(_create_expected_tf_config('worker', i))},
      )

    # Test parameter servers
    for i in range(num_ps):
      ps_job_name = expected_ps_job_names[i]
      job = cast(xm.Job, job_group.jobs[ps_job_name])

      self.assertEqual(job.executable, ps_executable)
      self.assertEqual(job.executor, ps_executor)
      self.assertEqual(
          job.env_vars,
          {'TF_CONFIG': json.dumps(_create_expected_tf_config('ps', i))},
      )


if __name__ == '__main__':
  absltest.main()
