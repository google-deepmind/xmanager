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
"""Interface for launching Vizier Explorations using Vertex Vizier."""

import asyncio
from typing import Any, Dict

from xmanager import xm
from xmanager.vizier.vizier_cloud import study_factory as sf
from xmanager.vizier.vizier_cloud import vizier_controller

_DEFAULT_LOCATION = 'us-central1'


# TODO: Add vizier_controller as auxiliary Job generator.
class VizierExploration:
  """An API for launching experiment as a Vizier-based Exploration."""

  def __init__(self, experiment: xm.Experiment, job: xm.JobType,
               study_factory: sf.StudyFactory, num_trials_total: int,
               num_parallel_trial_runs: int) -> None:
    """Create a VizierExploration.

    Args:
      experiment: the experiment who does the exploration.
      job: a job to run.
      study_factory: the VizierStudyFactory used to create or load the study.
      num_trials_total: total number of trials the experiment want to explore.
      num_parallel_trial_runs: number of parallel runs evaluating the trials.
    """

    # TODO: Reconsider to make functions async instead of
    # using get_event_loop().
    def work_unit_generator(vizier_params: Dict[str, Any]) -> xm.WorkUnit:
      return asyncio.get_event_loop().run_until_complete(
          experiment.add(job, self._to_job_params(vizier_params)))

    if not study_factory.display_name:
      study_factory.display_name = f'X{experiment.experiment_id}'

    self._controller = vizier_controller.VizierController(
        work_unit_generator, study_factory.vz_client, study_factory.study(),
        num_trials_total, num_parallel_trial_runs)

  def _to_job_params(self, vizier_params: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: unflatten parameters for JobGroup case (currently this
    # works for xm.Job).
    # For example: transform
    # {'learner.args.learning_rate': 0.1}
    # to
    # {'learner': {'args': {'learning_rate': 0.1}}}
    return {'args': vizier_params}

  def launch(self, **kwargs) -> None:
    self._controller.run(**kwargs)
