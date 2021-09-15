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
r"""XManager launcher for Polynomial.

Usage:

xmanager launch examples/vizier/launcher.py -- \
    --xm_wrap_late_bindings
"""

from absl import app

from google.cloud import aiplatform_v1beta1 as aip

from xmanager import vizier
from xmanager import xm
from xmanager import xm_local


def get_study_spec() -> aip.StudySpec:
  return aip.StudySpec(
      algorithm=aip.StudySpec.Algorithm.RANDOM_SEARCH,
      parameters=[
          aip.StudySpec.ParameterSpec(
              parameter_id='x',
              double_value_spec=aip.StudySpec.ParameterSpec.DoubleValueSpec(
                  min_value=-2.0, max_value=2.0)),
          aip.StudySpec.ParameterSpec(
              parameter_id='y',
              double_value_spec=aip.StudySpec.ParameterSpec.DoubleValueSpec(
                  min_value=-2.0, max_value=2.0))
      ],
      metrics=[
          aip.StudySpec.MetricSpec(
              metric_id='loss', goal=aip.StudySpec.MetricSpec.GoalType.MINIMIZE)
      ])


def main(_):
  with xm_local.create_experiment(experiment_title='polynomial') as experiment:
    spec = xm.PythonContainer(
        # Package the current directory that this script is in.
        path='.',
        base_image='gcr.io/deeplearning-platform-release/base-cpu',
        entrypoint=xm.ModuleName('polynomial'),
    )

    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=spec,
            executor_spec=xm_local.Caip.Spec(),
        ),
    ])

    vizier.VizierExploration(
        experiment=experiment,
        job=xm.Job(
            executable=executable,
            executor=xm_local.Caip(),
        ),
        study_factory=vizier.NewStudy(study_spec=get_study_spec()),
        num_trials_total=3,
        num_parallel_trial_runs=2).launch()


if __name__ == '__main__':
  app.run(main)
