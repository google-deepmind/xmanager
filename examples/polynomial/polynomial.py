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
"""Compute the value of a bivariate quadratic polynomial.

An example of finding the min values of x and y with auto hyperparam tuning.
Set a, b, c, d, e, and f to constant args. Set x and y to be hyperparameters.
"""

from absl import app
from absl import flags

from vizier_worker import VizierWorker

FLAGS = flags.FLAGS
flags.DEFINE_integer('a', 1, 'a in ax^2 + by^2 + cxy + dx + ey + f')
flags.DEFINE_integer('b', 1, 'b in ax^2 + by^2 + cxy + dx + ey + f')
flags.DEFINE_integer('c', 0, 'c in ax^2 + by^2 + cxy + dx + ey + f')
flags.DEFINE_integer('d', 1, 'd in ax^2 + by^2 + cxy + dx + ey + f')
flags.DEFINE_integer('e', 1, 'e in ax^2 + by^2 + cxy + dx + ey + f')
flags.DEFINE_integer('f', 1, 'f in ax^2 + by^2 + cxy + dx + ey + f')

flags.DEFINE_float('x', 0, 'The hyperparameter variable X.')
flags.DEFINE_float('y', 0, 'The hyperparameter variable Y.')

flags.DEFINE_string(
    'trial_name', None, 'Identifying the current job trial that measurements '
    'will be submitted to with `add_trial_measurement`. Format: '
    'projects/{project}/locations/{location}/studies/{study}/trials/{trial}')


def main(_):
  worker = VizierWorker(FLAGS.trial_name)

  # dummy training loop: "train" for one epoch
  metric_value = float(
      FLAGS.a * FLAGS.x * FLAGS.x + FLAGS.b * FLAGS.y * FLAGS.y +
      FLAGS.c * FLAGS.x * FLAGS.y + FLAGS.d * FLAGS.x + FLAGS.e * FLAGS.y +
      FLAGS.f)

  worker.add_trial_measurement(1, {'loss': metric_value})

if __name__ == '__main__':
  app.run(main)
