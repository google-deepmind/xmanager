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
r"""XManager launcher for CIFAR10.

Usage:

xmanager launch examples/cifar10_torch/launcher.py -- \
  --xm_wrap_late_bindings \
  [--image_path=gcr.io/path/to/image/tag] \
  [--platform=gpu]
"""
import itertools

from absl import app
from absl import flags
from xmanager import xm
from xmanager import xm_local

FLAGS = flags.FLAGS
flags.DEFINE_string('image_path', None, 'Image path.')
flags.DEFINE_string('platform', 'cpu', 'cpu/gpu/tpu.')
flags.DEFINE_integer('cores', 1, 'Number of cores. Use 8 if platform==tpu.')


def main(_):
  with xm_local.create_experiment(experiment_title='cifar10') as experiment:
    if FLAGS.image_path:
      spec = xm.Container(image_path=FLAGS.image_path)
    else:
      # Package the current directory that this script is in.
      spec = xm.PythonContainer(
          path='.',
          # This base_image is experimental and works with cpu/gpu/tpu.
          # https://cloud.google.com/ai-platform/deep-learning-containers/docs/choosing-container
          base_image='gcr.io/deeplearning-platform-release/pytorch-xla.1-8',
          entrypoint=xm.ModuleName('cifar10'),
      )

    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=spec,
            executor_spec=xm_local.Vertex.Spec(),
            args={'platform': FLAGS.platform},
        ),
    ])

    batch_sizes = [64, 1024]
    learning_rates = [0.1, 0.001]
    trials = list(
        dict([('batch_size', bs), ('learning_rate', lr)])
        for (bs, lr) in itertools.product(batch_sizes, learning_rates))

    requirements = xm.JobRequirements()
    if FLAGS.platform == 'gpu':
      requirements = xm.JobRequirements(t4=FLAGS.cores)
    elif FLAGS.platform == 'tpu':
      requirements = xm.JobRequirements(tpu_v3=8)
    for hyperparameters in trials:
      jobs = {}
      jobs['coordinator'] = xm.Job(
          executable=executable,
          executor=xm_local.Vertex(requirements),
          args=hyperparameters,
      )
      experiment.add(xm.JobGroup(**jobs))
      break
    print('Waiting for async launches to return values...')
  print('Launch completed and successful.')


if __name__ == '__main__':
  app.run(main)
