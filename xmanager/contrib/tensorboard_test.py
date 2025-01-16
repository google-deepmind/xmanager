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
"""Tests for the xmanager.contrib.tensoboard module."""

from typing import Any, Mapping

from absl.testing import absltest
from absl.testing import parameterized
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import tensorboard


class TensorboardTest(parameterized.TestCase):

  @parameterized.product(
      executor=[xm_local.Vertex(), xm_local.Kubernetes(), xm_local.Local()],
      logdir=['logs'],
      args=[{}, None, {'new': 0}],
  )
  def test_add_tensorboard_negative_timeout(
      self, executor: xm.Executor, logdir: str, args: Mapping[str, Any]
  ):
    mock_experiment = absltest.mock.Mock()
    mock_experiment.add.return_value = None
    mock_executable = absltest.mock.Mock()
    mock_experiment.package.return_value = [mock_executable]

    with self.assertRaises(RuntimeError):
      tensorboard.add_tensorboard(
          mock_experiment,
          logdir=logdir,
          executor=executor,
          timeout_secs=-5,
          args=args,
      )

  @parameterized.product(
      executor=[xm_local.Vertex(), xm_local.Kubernetes(), xm_local.Local()],
      logdir=['logs'],
      timeout_secs=[0, 5],
      args=[{}, None, {'new': 0}],
  )
  def test_add_tensorboard(
      self,
      executor: xm.Executor,
      logdir: str,
      timeout_secs: int,
      args: Mapping[str, Any],
  ):
    mock_experiment = absltest.mock.Mock()
    mock_experiment.add.return_value = None
    mock_executable = absltest.mock.Mock()
    mock_experiment.package.return_value = [mock_executable]

    tensorboard.add_tensorboard(
        mock_experiment,
        logdir=logdir,
        executor=executor,
        timeout_secs=timeout_secs,
        args=args,
    )

    expected_packageable_spec_arg = (
        tensorboard.TensorboardProvider.get_tensorboard_packageable(
            timeout_secs=timeout_secs
        )
    )
    mock_experiment.package.assert_called_once()
    packageable_arg = mock_experiment.package.call_args[0][0][0]
    self.assertEqual(
        packageable_arg.executable_spec, expected_packageable_spec_arg
    )
    self.assertEqual(packageable_arg.executor_spec, executor.Spec())

    expected_job = xm.Job(
        executable=mock_executable,
        executor=executor,
        args=tensorboard.TensorboardProvider.get_tensorboard_job_args(
            logdir, additional_args=args
        ),
        name='tensorboard',
    )
    mock_experiment.add.assert_called_once()
    auxiliary_job_arg = mock_experiment.add.call_args[0][0]
    self.assertIsInstance(auxiliary_job_arg, xm.AuxiliaryUnitJob)
    self.assertEqual(auxiliary_job_arg._job, expected_job)

  def test_get_tensorboard_packageable_negative_timeout(self):
    provider = tensorboard.TensorboardProvider
    with self.assertRaises(RuntimeError):
      provider.get_tensorboard_packageable(timeout_secs=-5)

  @parameterized.product(timeout_secs=[0, 5])
  def test_get_tensorboard_packageable(self, timeout_secs: int):
    provider = tensorboard.TensorboardProvider

    packageable = provider.get_tensorboard_packageable(
        timeout_secs=timeout_secs
    )

    self.assertIsInstance(packageable, xm.PythonContainer)
    self.assertEqual(packageable.base_image, 'tensorflow/tensorflow')

  @parameterized.product(
      log_dir=['logs'],
      port=[None, 6006, 2002],
      additional_args=[{}, None, {'logdir': 'logs_v2'}, {'new': 0}],
  )
  def test_get_tensorboard_job_args(
      self, log_dir: str, port: int, additional_args: Mapping[str, Any]
  ):
    provider = tensorboard.TensorboardProvider

    args = provider.get_tensorboard_job_args(log_dir, port, additional_args)
    expected = dict(
        {
            'logdir': log_dir,
            'port': port or provider.DEFAULT_TENSORBOARD_PORT,
        },
        **(additional_args if additional_args else {}),
    )
    self.assertEqual(args, {**args, **expected})


if __name__ == '__main__':
  absltest.main()
