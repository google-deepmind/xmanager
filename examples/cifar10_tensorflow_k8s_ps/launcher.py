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
r"""XManager launcher for CIFAR10 using TF's ParameterServerStrategy.

Usage:

xmanager launch examples/cifar10_tensorflow_k8s__ps/launcher.py -- \
  --xm_wrap_late_bindings [--image_path=gcr.io/path/to/image/tag]
"""
import itertools

from absl import app
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import xm_tensorflow


def main(_):
  with xm_local.create_experiment(
      experiment_title='kubernetes_multiworker'
  ) as experiment:
    spec = xm.PythonContainer(
        # Package the current directory that this script is in.
        path='.',
        base_image='gcr.io/deeplearning-platform-release/tf2-gpu.2-6',
        entrypoint=xm.ModuleName('cifar10'),
    )

    [executable] = experiment.package(
        [
            xm.Packageable(
                executable_spec=spec,
                executor_spec=xm_local.Kubernetes.Spec(),
                args={},
            )
        ]
    )

    learning_rates = [0.001]
    trials = list(
        dict([('learning_rate', lr)])
        for (lr,) in itertools.product(learning_rates)
    )

    builder = xm_tensorflow.ParameterServerStrategyBuilder(
        experiment=experiment,
        chief_executable=executable,
        chief_executor=xm_local.Kubernetes(
            requirements=xm.JobRequirements(t4=1)
        ),
        worker_executable=executable,
        worker_executor=xm_local.Kubernetes(
            requirements=xm.JobRequirements(t4=1)
        ),
        worker_name='worker',
        ps_executable=executable,
        ps_executor=xm_local.Kubernetes(),
        ps_name='ps',
        num_workers=2,
        num_ps=1,
    )

    for hyperparameters in trials:
      experiment.add(builder.gen_job_group(), args=hyperparameters)


if __name__ == '__main__':
  app.run(main)
