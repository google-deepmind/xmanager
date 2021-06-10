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

  def test_construct_requirements(self):
    requirements = resources.JobRequirements({resources.ResourceType.CPU: 4},
                                             v100=1)
    task_requirements = requirements.task_requirements
    self.assertEqual(task_requirements[resources.ResourceType.CPU], 4)
    self.assertEqual(task_requirements[resources.ResourceType.V100], 1)

  def test_resource_specified_twice(self):
    with self.assertRaises(ValueError):
      resources.JobRequirements({resources.ResourceType.CPU: 1}, cpu=2)

  def test_tpu_job(self):
    requirements = resources.JobRequirements(tpu_v3='4x4')
    self.assertEqual(requirements.accelerator, resources.ResourceType.TPU_V3)
    self.assertEqual(requirements.topology.name, '4x4')

  def test_location(self):
    requirements = resources.JobRequirements(location='lon_r7')
    self.assertEqual(requirements.location, 'lon_r7')

  def test_service_tier(self):
    requirements = resources.JobRequirements(
        service_tier=resources.ServiceTier.PROD)
    self.assertEqual(requirements.service_tier, resources.ServiceTier.PROD)


class EnumSubsetTest(parameterized.TestCase):

  def test_construction(self):
    self.assertEqual(resources.GpuType['V100'], resources.GpuType(17))
    with self.assertRaises(AttributeError):
      resources.GpuType['TPU_V3']  # pylint: disable=pointless-statement
    with self.assertRaises(ValueError):
      resources.GpuType(170)

  def test_equivalence(self):
    self.assertEqual(resources.ResourceType.V100, resources.GpuType.V100)

  def test_iteration(self):
    gpus = set(iter(resources.GpuType))
    self.assertIn(resources.ResourceType.V100, gpus)
    self.assertNotIn(resources.ResourceType.TPU_V3, gpus)

  def test_contains(self):
    self.assertIn(resources.ResourceType.V100, resources.GpuType)
    self.assertNotIn(resources.ResourceType.TPU_V3, resources.GpuType)


if __name__ == '__main__':
  unittest.main()
