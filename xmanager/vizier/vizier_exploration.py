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

import abc
import asyncio
from typing import Any, Dict, Optional

from google.cloud import aiplatform_v1beta1 as aip

from xmanager import xm
from xmanager.cloud import auth
from xmanager.vizier.vizier_controller import VizierController

_DEFAULT_LOCATION = 'us-central1'


class VizierStudyFactory(abc.ABC):
  """Abstract class representing vizier study generator."""

  def __init__(self, location: str) -> None:
    self.vz_client = aip.VizierServiceClient(
        client_options=dict(
            api_endpoint=f'{location}-aiplatform.googleapis.com'))

  @abc.abstractmethod
  def study(self, experiment: xm.Experiment) -> aip.Study:
    """Create or load the study for the given `experiment`."""


class NewStudy(VizierStudyFactory):
  """Vizier study generator that generates new study from given config."""

  def __init__(self,
               study_spec: aip.StudySpec,
               display_name: Optional[str] = None,
               project: str = auth.get_project_name(),
               location: str = _DEFAULT_LOCATION) -> None:
    super().__init__(location)

    self._display_name = display_name
    self._study_spec = study_spec
    self._project = project
    self._location = location

  def study(self, experiment: xm.Experiment) -> aip.Study:
    return self.vz_client.create_study(
        parent=f'projects/{self._project}/locations/{self._location}',
        study=aip.Study(
            display_name=self._display_name or f'X{experiment.experiment_id}',
            study_spec=self._study_spec))


class VizierExploration:
  """An API for launching experiment as a Vizier-based Exploration."""

  def __init__(self, experiment: xm.Experiment, job: xm.JobType,
               study_factory: VizierStudyFactory, num_trials_total: int,
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

    self._controller = VizierController(work_unit_generator,
                                        study_factory.vz_client,
                                        study_factory.study(experiment),
                                        num_trials_total,
                                        num_parallel_trial_runs)

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
