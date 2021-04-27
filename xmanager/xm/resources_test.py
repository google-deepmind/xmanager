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
"""Tests for xmanager.xm.resources."""

import unittest

from xmanager import xm
from xmanager.xm import resources
from xmanager.xm.resources import JobRequirements
from xmanager.xm.resources import ResourceType


class ResourceDictTest(unittest.TestCase):

  def test_resource_dict_to_string(self):
    resource_dict = resources.ResourceDict()
    resource_dict[ResourceType.V100] = 8
    resource_dict[ResourceType.CPU] = 4.2 * xm.vCPU
    resource_dict[ResourceType.MEMORY] = 16.5 * xm.GiB

    self.assertEqual(
        str(resource_dict), 'CPU: 4.2, MEMORY: 17716740096.0, V100: 8')

  def test_resource_dict_from_job_requirements(self):
    requirements = JobRequirements(cpu=0.5 * xm.vCPU, memory=2 * xm.MiB, v100=8)
    resource_dict = requirements.task_requirements
    self.assertEqual(resource_dict[ResourceType.CPU], 0.5)
    self.assertEqual(resource_dict[ResourceType.MEMORY], 2097152)
    self.assertEqual(resource_dict[ResourceType.V100], 8)

  def test_job_requirements_unknown_key(self):
    with self.assertRaises(KeyError):
      JobRequirements(cpu=0.5 * xm.vCPU, upc=2)


if __name__ == '__main__':
  unittest.main()
