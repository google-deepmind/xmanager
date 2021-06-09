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
"""Vizier Controller."""

import time
from typing import Any, Callable, Dict, Optional

from google.cloud import aiplatform_v1beta1 as aip

from xmanager import xm


class VizierController:
  """A Controller that runs Vizier suggested hyperparameters in multiple work units."""

  def __init__(self, work_unit_generator: Callable[[Dict[str, Any]],
                                                   xm.WorkUnit],
               vz_client: aip.VizierServiceClient, study: aip.Study,
               num_work_units_total: int, num_parallel_work_units: int) -> None:
    """Create a VizierController.

    Args:
      work_unit_generator: the function that generates WorkUnit from
        hyperparameters.
      vz_client: the Vizier Client used for interacting with Vizier.
      study: the study the controller works on.
      num_work_units_total: number of work units to create in total. (TODO:
        remove this and retrieve from study spec stopping criteria once it is
        settable there.)
      num_parallel_work_units: number of work units to run in parallel.
    """
    self._work_unit_generator = work_unit_generator
    self._vz_client = vz_client
    self._study = study
    self._num_work_units_total = num_work_units_total
    self._num_parallel_work_units = num_parallel_work_units

    self._work_units = []
    self._trials = []

  def run(self, poll_frequency_in_sec: float = 60) -> None:
    """Peridically check and sync status between vizier and work units and create new work units when needed."""
    while True:
      # 1. Complete trial for completed work unit;
      # TODO: i. Early-stop first if needed.
      # ii. Add infeasible_reason if available.
      for i in range(len(self._work_units)):
        if not self._work_units[i].get_status().is_running():
          self._complete_trial(self._trials[i])

      # 2. TODO: Return by Vizier's indication that study is done
      # when such API is ready on Vizier side.
      if len(self._work_units) == self._num_work_units_total and sum([
          not wu.get_status().is_running() for wu in self._work_units
      ]) == self._num_work_units_total:
        print('All done! Exiting VizierController... \n')
        return

      # 3. Get new trials and assign to new work units.
      self._launch_new_work_units()

      time.sleep(poll_frequency_in_sec)

  def _launch_new_work_units(self) -> None:
    """Get hyperparmeter suggestions from Vizier and assign to new work units to run."""
    # 1. Compute num of work units to create next.
    num_existing_work_units = len(self._work_units)
    num_running_work_units = len(
        [wu for wu in self._work_units if wu.get_status().is_running()])
    num_work_units_to_create_total = self._num_work_units_total - num_existing_work_units
    num_work_units_to_create_next = min(
        self._num_parallel_work_units - num_running_work_units,
        num_work_units_to_create_total)

    # 2. Create the work units.
    start_index = num_existing_work_units + 1
    for i in range(start_index, start_index + num_work_units_to_create_next):
      trial = self._vz_client.suggest_trials(
          request=aip.SuggestTrialsRequest(
              parent=self._study.name,
              suggestion_count=1,
              client_id=f'work unit {i}')).result().trials[0]
      print(f'Trial for work unit (index: {i}) is retrieved：\n{trial}')

      print(f'Creating work unit (index: {i})... \n')

      work_unit = self._work_unit_generator({
          'trial_name': trial.name,
          **{p.parameter_id: p.value for p in trial.parameters}
      })

      # TODO: Add an utility to handle logging conditionally
      # (use print when run local otherwise logging.info.)
      print(f'Work unit (index: {i}, id: {work_unit.work_unit_id}) created. \n')

      self._work_units.append(work_unit)
      self._trials.append(trial)

  def _complete_trial(self,
                      trial: aip.Trial,
                      infeasible_reason: Optional[str] = None) -> None:
    """Complete a trial."""
    self._vz_client.complete_trial(
        request=aip.CompleteTrialRequest(
            name=trial.name,
            trial_infeasible=infeasible_reason is not None,
            infeasible_reason=infeasible_reason))
    print(f'Trial {trial.name} is completed')
