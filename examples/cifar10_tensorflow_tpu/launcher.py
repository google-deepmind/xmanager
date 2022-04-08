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

xmanager launch examples/cifar10_tensorflow_tpu/launcher.py
"""
import asyncio
import itertools
import os

from absl import app
from absl import flags
from xmanager import xm
from xmanager import xm_local
from xmanager.cloud import build_image
from xmanager.cloud import vertex
from xmanager.contrib import tpu

FLAGS = flags.FLAGS
flags.DEFINE_string('tensorboard', None, 'Tensorboard instance.')


def main(_):
  with xm_local.create_experiment(experiment_title='cifar10') as experiment:
    directory = os.path.basename(os.path.dirname(os.path.realpath(__file__)))
    # pyformat: disable
    spec = xm.PythonContainer(
        # Package the current directory that this script is in.
        path='.',
        # tpuvm requires Python3.8 and GLIBC_2.29, which requires at least
        # debian:11 or ubuntu:20.04
        base_image='ubuntu:20.04',
        docker_instructions=(
            ['RUN apt-get update && apt-get install -y python-is-python3 python3-pip wget'] +  # pylint: disable=line-too-long
            build_image.default_steps(directory, use_deep_module=False) +
            tpu.tpuvm_docker_instructions()),
        entrypoint=xm.ModuleName('cifar10'),
    )
    # pyformat: enable

    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=spec,
            executor_spec=xm_local.Vertex.Spec(),
            args={},
        ),
    ])

    learning_rates = [0.1, 0.001]
    trials = list(
        dict([('learning_rate', lr)])
        for (lr,) in itertools.product(learning_rates))

    tensorboard = FLAGS.tensorboard
    if not tensorboard:
      tensorboard = vertex.get_default_client().get_or_create_tensorboard('cifar10')
      tensorboard = asyncio.get_event_loop().run_until_complete(tensorboard)

    for i, hyperparameters in enumerate(trials):
      output_dir = os.environ.get('GOOGLE_CLOUD_BUCKET_NAME', None)
      if output_dir:
        output_dir = os.path.join(output_dir, str(experiment.experiment_id),
                                  str(i))
      tensorboard_capability = xm_local.TensorboardCapability(
          name=tensorboard, base_output_directory=output_dir)
      experiment.add(
          xm.Job(
              executable=executable,
              executor=xm_local.Vertex(
                  requirements=xm.JobRequirements(tpu_v2=8),
                  tensorboard=tensorboard_capability),
              args=hyperparameters,
          ))


if __name__ == '__main__':
  app.run(main)
