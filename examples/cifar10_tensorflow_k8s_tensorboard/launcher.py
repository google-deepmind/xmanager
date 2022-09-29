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
r"""XManager launcher for running CIFAR10 on Kubernetes with Tensorboard auxiliary job.

Usage:

xmanager launch examples/cifar10_tensorflow_k8s_tensorboard/launcher.py -- \
  --tensorboard_log_dir=TENSORBOARD_LOG_DIR \
  [--tensorboard_timeout_secs=TIMEOUT_SECS]
"""
import itertools
import os

from absl import app
from absl import flags
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import tensorboard

_TENSORBOARD_LOG_DIR = flags.DEFINE_string(
    'tensorboard_log_dir', None,
    'Log directory to be used by workers and Tensorboard.')

_TENSORBOARD_TIMEOUT_SECS = flags.DEFINE_integer(
    'tensorboard_timeout_secs', 60 * 60,
    'The amount of time the Tensorboard job should run for.')


def main(_):
  with xm_local.create_experiment(experiment_title='cifar10') as experiment:
    spec = xm.PythonContainer(
        # Package the current directory that this script is in.
        path='.',
        base_image='gcr.io/deeplearning-platform-release/tf2-gpu.2-6',
        entrypoint=xm.ModuleName('cifar10'),
    )

    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=spec,
            executor_spec=xm_local.Kubernetes.Spec(),
        ),
    ])

    learning_rates = [0.1, 0.001]
    trials = list(
        dict([('learning_rate', lr)])
        for (lr,) in itertools.product(learning_rates))

    log_dir = None
    if _TENSORBOARD_LOG_DIR.value:
      log_dir = f'{_TENSORBOARD_LOG_DIR.value}/{str(experiment.experiment_id)}/logs'

    if log_dir:
      tensorboard.add_tensorboard(
          experiment,
          log_dir,
          executor=xm_local.Kubernetes(),
          timeout_secs=_TENSORBOARD_TIMEOUT_SECS.value)

    for i, hyperparameters in enumerate(trials):
      output_dir = os.path.join(log_dir, str(i))
      experiment.add(
          xm.Job(
              executable=executable,
              executor=xm_local.Kubernetes(),
              args=dict({
                  'tensorboard_log_dir': output_dir,
                  **hyperparameters
              })))


if __name__ == '__main__':
  app.run(main)
