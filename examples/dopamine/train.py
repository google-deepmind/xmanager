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
"""The entry point for running a Dopamine agent."""
import os

from absl import app
from absl import flags
from absl import logging

from dopamine.discrete_domains import run_experiment
import tensorflow as tf

FLAGS = flags.FLAGS
flags.DEFINE_multi_string(
    'gin_files',
    [],
    (
        'List of paths to gin configuration files (e.g.'
        '"dopamine/agents/dqn/dqn.gin").'
    ),
)

# When using Vertex Tensorboard, the tensorboard will be present as a
# environment variable.
BASE_DIR = os.environ.get('AIP_TENSORBOARD_LOG_DIR', '/tmp/dopamine_runs')


def main(unused_argv):
  logging.set_verbosity(logging.INFO)
  tf.compat.v1.disable_v2_behavior()
  run_experiment.load_gin_configs(FLAGS.gin_files, [])
  runner = run_experiment.create_runner(BASE_DIR)
  runner.run_experiment()


if __name__ == '__main__':
  app.run(main)
