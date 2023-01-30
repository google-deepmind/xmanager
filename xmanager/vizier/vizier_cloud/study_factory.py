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
"""Factory classes for generating study of cloud Vertex Vizier."""

import abc
from typing import Optional

from google.cloud import aiplatform_v1beta1 as aip

from xmanager.cloud import auth

_DEFAULT_LOCATION = 'us-central1'


class StudyFactory(abc.ABC):
  """Abstract class representing vizier study generator."""

  vz_client: aip.VizierServiceClient
  study_config: aip.StudySpec
  num_trials_total: int
  display_name: str

  # TODO: Once vertex pyvizier is available, we should replace
  # aip.StudySpec with it.
  # display_name and num_trials_total are supposed to be set into the study
  # config, which is not supported by aip.StudySpec currently. But should be
  # settable when pyvizier.StudyConfig is available.
  def __init__(
      self,
      study_config: aip.StudySpec,
      num_trials_total: int,
      display_name: str,
      location: str,
  ) -> None:
    super().__init__()
    self.study_config = study_config
    self.num_trials_total = num_trials_total
    self.display_name = display_name
    self.vz_client = aip.VizierServiceClient(
        client_options=dict(
            api_endpoint=f'{location}-aiplatform.googleapis.com'
        )
    )

  @abc.abstractmethod
  def study(self) -> str:
    raise NotImplementedError


class NewStudy(StudyFactory):
  """Vizier study generator that generates new study from given config."""

  project: str
  location: str

  # `num_trials_total` is a required field. Default it to 0 to unbreak the
  # soon-to-deprecate VizierExploration users.
  # `display_name` is optional for user to customize, if not set, XM will
  # set it with experiment information
  def __init__(
      self,
      study_config: aip.StudySpec,
      num_trials_total: int = 0,
      display_name: Optional[str] = None,
      project: Optional[str] = None,
      location: Optional[str] = None,
  ) -> None:
    self.project = project or auth.get_project_name()
    self.location = location or _DEFAULT_LOCATION

    super().__init__(
        study_config, num_trials_total, display_name or '', self.location
    )

  def study(self) -> str:
    return self.vz_client.create_study(
        parent=f'projects/{self.project}/locations/{self.location}',
        study=aip.Study(
            display_name=self.display_name, study_spec=self.study_config
        ),
    ).name
