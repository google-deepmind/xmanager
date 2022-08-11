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
r"""XManager local launcher for CIFAR10 using GPUs.

Usage:

xmanager launch examples/local_container_gpu/launcher.py -- \
  --xm_wrap_late_bindings
"""

from absl import app
from absl import flags
from xmanager import xm
from xmanager import xm_local

_EXP_NAME = flags.DEFINE_string(
    'exp_name', 'local-cifar10-gpu', 'Name of the experiment.', short_name='n')
_INTERACTIVE = flags.DEFINE_bool(
    'interactive', False,
    'Launch the container and allow interactive access to it.')


def main(argv) -> None:
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  create_experiment = xm_local.create_experiment
  with create_experiment(experiment_title=_EXP_NAME.value) as experiment:
    docker_options = xm_local.DockerOptions(
        interactive=_INTERACTIVE.value)
    # Creating local executor with extra flag to track job's progress.
    executor = xm_local.Local(
        xm.JobRequirements(local_gpu=2),
        experimental_stream_output=True, docker_options=docker_options)

    # Empty args means nothing is passed into the job.
    executable_args = {}
    executable, = experiment.package([
        xm.python_container(
            executor_spec=executor.Spec(),
            args=executable_args,
            # Package the current directory that this script is in.
            path='.',
            base_image='gcr.io/deeplearning-platform-release/tf2-gpu.2-6',
            entrypoint=xm.ModuleName('local_container_gpu.cifar10'),
            use_deep_module=True,
        )
    ])
    job = xm.Job(executable, executor)

    experiment.add(job)


if __name__ == '__main__':
  app.run(main)
