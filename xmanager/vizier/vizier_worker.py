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
"""Run Job as a Vizier worker to manager WorkUnit Vizier interaction."""
import re
from typing import Dict, Optional

from absl import logging
from google.cloud import aiplatform_v1beta1 as aip

_TRIAL_NAME_REGEX = r'projects\/[^\/]+\/locations\/[^\/]+\/studies\/[^\/]+\/trials\/[^\/]+'


class VizierWorker:
  """Worker that manage interaction between job and Vizier."""

  def __init__(self, trial_name: str) -> None:
    if not re.match(_TRIAL_NAME_REGEX, trial_name):
      raise Exception('The trial_name must be in the form: '
                      'projects/{project}/locations/{location}/'
                      'studies/{study}/trials/{trial}')

    self._trial_name = trial_name

    location = trial_name.split('/')[3]
    self._vz_client = aip.VizierServiceClient(client_options={
        'api_endpoint': f'{location}-aiplatform.googleapis.com',
    })

  def add_trial_measurement(self, step: int, metrics: Dict[str, float]) -> None:
    """Add trial measurements to Vizier."""
    self._vz_client.add_trial_measurement(
        request=aip.AddTrialMeasurementRequest(
            trial_name=self._trial_name,
            measurement=aip.Measurement(
                step_count=step,
                metrics=[
                    aip.Measurement.Metric(metric_id=k, value=v)
                    for k, v in metrics.items()
                ],
            )))
    logging.info('Step %d Metric %s is reported', step, metrics)

  def complete_trial(self, infeasible_reason: Optional[str] = None) -> None:
    """Complete a trial."""
    self._vz_client.complete_trial(
        request=aip.CompleteTrialRequest(
            name=self._trial_name,
            trial_infeasible=infeasible_reason is not None,
            infeasible_reason=infeasible_reason))
    logging.info('Trial %s is completed', self._trial_name)
