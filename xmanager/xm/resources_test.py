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
from absl.testing import parameterized

from xmanager import xm
from xmanager.xm import resources
from xmanager.xm.resources import JobRequirements
from xmanager.xm.resources import ResourceType


class ResourceDictTest(unittest.TestCase):

  def test_resource_type_by_name(self):
    self.assertEqual(ResourceType['cpu'], ResourceType.CPU)
    self.assertEqual(ResourceType['Cpu'], ResourceType.CPU)
    self.assertEqual(ResourceType['CPU'], ResourceType.CPU)
    with self.assertRaises(KeyError):
      ResourceType['UPC']  # pylint: disable=pointless-statement

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


class TopologyTest(parameterized.TestCase):

  @parameterized.parameters(('2', 2), ('4x4', 16), ('2x3x5', 30),
                            ('4x4_twisted', 16))
  def test_resource_type_by_name(self, topology, chip_count):
    self.assertEqual(resources.Topology(topology).chip_count, chip_count)

  def test_invalid_topology(self):
    with self.assertRaises(resources.InvalidTpuTopologyError):
      resources.Topology('euclidian')


class JobRequirementsTest(parameterized.TestCase):

  def test_cpu_job(self):
    requirements = resources.JobRequirements(cpu=1.2, RAM=1 * xm.GiB)
    self.assertEqual(requirements.task_requirements[resources.ResourceType.CPU],
                     1.2)
    self.assertEqual(requirements.task_requirements[resources.ResourceType.RAM],
                     1 * xm.GiB)
    self.assertIsNone(requirements.accelerator)
    self.assertIsNone(requirements.topology)

  def test_tpu_job(self):
    requirements = resources.JobRequirements(tpu_v3='4x4')
    self.assertEqual(requirements.accelerator, resources.ResourceType.TPU_V3)
    self.assertEqual(requirements.topology.name, '4x4')


if __name__ == '__main__':
  unittest.main()
