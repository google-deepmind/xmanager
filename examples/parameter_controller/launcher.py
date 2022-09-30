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
r"""XManager launcher for a parameter controller example.

The given program launches a dummy job on VertexAI, waits for its completion
and then launches another job on Kubernetes. This kind of workflow can be used
to define pipelines.

Usage:


xmanager launch examples/parameter_controller/launcher.py -- \
  --xm_db_yaml_config=db_config.yaml
  [--xm_k8s_service_account_name=...]
  [--xm_gcp_service_account_name=...]

The content of `db_config.yaml` must be updated to match the connection details
to the DB used.
"""
import os

from absl import app
from absl import flags
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import parameter_controller


def main(_):
  with xm_local.create_experiment(experiment_title='cifar10') as experiment:

    @parameter_controller.controller(
        executor=xm_local.Kubernetes(),
        controller_args={
            'xm_k8s_service_account_name':
                flags.FLAGS.xm_k8s_service_account_name,
            'xm_gcp_service_account_name':
                flags.FLAGS.xm_gcp_service_account_name,
        },
        controller_env_vars={
            'GOOGLE_CLOUD_BUCKET_NAME': os.environ['GOOGLE_CLOUD_BUCKET_NAME'],
        },
        # Package contents of this directory inside parameter controller job
        package_path='.')
    async def parameter_controller_example(experiment: xm.Experiment):
      spec = xm.PythonContainer(
          # Package contents of job to be launched
          path='inner_job',
          base_image='python:3.9',
          entrypoint=xm.ModuleName('wait_job'),
      )
      [vertex_executable, k8s_executable] = experiment.package([
          xm.Packageable(
              executable_spec=spec,
              executor_spec=xm_local.Vertex.Spec(),
              args={'time_to_sleep': 10},
          ),
          xm.Packageable(
              executable_spec=spec,
              executor_spec=xm_local.Kubernetes.Spec(),
              args={'time_to_sleep': 20},
          ),
      ])

      wu1 = await experiment.add(xm.Job(k8s_executable, xm_local.Kubernetes()))
      await wu1.wait_until_complete()

      wu2 = await experiment.add(xm.Job(vertex_executable, xm_local.Vertex()))
      await wu2.wait_until_complete()

    experiment.add(parameter_controller_example())  # pylint: disable=no-value-for-parameter


if __name__ == '__main__':
  app.run(main)
