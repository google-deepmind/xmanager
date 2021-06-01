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
"""Implementation of local work unit statuses."""
import enum

import attr

from xmanager import xm


class LocalWorkUnitStatusEnum(enum.Enum):
  """Status of a local experiment job."""

  # Work unit was created, but has not terminated yet.
  RUNNING = 1
  # Work unit terminated and was successful.
  COMPLETED = 2
  # Work unit terminated and was not succesful.
  FAILED = 3
  # Work unit terminated because it was cancelled by the user.
  CANCELLED = 4


@attr.s(auto_attribs=True)
class LocalWorkUnitStatus(xm.WorkUnitStatus):
  """Status of a local experiment job."""

  status: LocalWorkUnitStatusEnum
  message: str = ''

  def is_running(self) -> bool:
    return self.status == LocalWorkUnitStatusEnum.RUNNING

  def is_succeeded(self) -> bool:
    return self.status == LocalWorkUnitStatusEnum.COMPLETED

  def is_failed(self) -> bool:
    return self.status == LocalWorkUnitStatusEnum.FAILED

  def error(self) -> str:
    return self.message
