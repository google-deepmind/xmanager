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

from absl.testing import absltest
from absl.testing import parameterized

from xmanager import xm
from xmanager.xm import resources
from xmanager.xm.resources import JobRequirements
from xmanager.xm.resources import ResourceType


class ResourceDictTest(absltest.TestCase):

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
        str(resource_dict), 'CPU: 4.2, MEMORY: 17716740096.0, V100: 8'
    )

  def test_resource_dict_from_job_requirements(self):
    requirements = JobRequirements(cpu=0.5 * xm.vCPU, memory=2 * xm.MiB, v100=8)
    resource_dict = requirements.task_requirements
    self.assertEqual(resource_dict[ResourceType.CPU], 0.5)
    self.assertEqual(resource_dict[ResourceType.MEMORY], 2097152)
    self.assertEqual(resource_dict[ResourceType.V100], 8)

  def test_job_requirements_unknown_key(self):
    with self.assertRaises(KeyError):
      JobRequirements(cpu=0.5 * xm.vCPU, upc=2)

  def test_requirements_summation(self):
    first = resources.JobRequirements(cpu=1, tpu_v2='2x2')
    second = resources.JobRequirements(ram=4 * xm.GiB, replicas=10)
    total = (
        first.replicas * first.task_requirements
        + second.replicas * second.task_requirements
    )
    self.assertEqual(total[ResourceType.CPU], 1)
    self.assertEqual(total[ResourceType.RAM], 40 * xm.GiB)
    self.assertEqual(total[ResourceType.TPU_V2], 4)


class TopologyTest(parameterized.TestCase):

  @parameterized.parameters(
      ('2', 2), ('4x4', 16), ('2x3x5', 30), ('4x4_twisted', 16)
  )
  def test_resource_type_by_name(self, topology, chip_count):
    self.assertEqual(resources.Topology(topology).chip_count, chip_count)

  def test_invalid_topology(self):
    with self.assertRaises(resources.InvalidTpuTopologyError):
      resources.Topology('euclidian')

  def test_topology_repr(self):
    self.assertEqual(repr(resources.Topology('4x4')), "xm.Topology('4x4')")

  def test_topology_eq(self):
    self.assertEqual(resources.Topology('4x4'), resources.Topology('4x4'))
    self.assertNotEqual(resources.Topology('2x2'), resources.Topology('4x4'))

    self.assertEqual(
        hash(resources.Topology('4x4')), hash(resources.Topology('4x4'))
    )


class JobRequirementsTest(parameterized.TestCase):

  def test_cpu_job(self):
    requirements = resources.JobRequirements(cpu=1.2, ram=1 * xm.GiB)
    self.assertEqual(
        requirements.task_requirements[resources.ResourceType.CPU], 1.2
    )
    self.assertEqual(
        requirements.task_requirements[resources.ResourceType.RAM], 1 * xm.GiB
    )
    self.assertIsNone(requirements.accelerator)
    self.assertIsNone(requirements.topology)
    self.assertEqual(requirements.replicas, 1)

  def test_construct_requirements(self):
    requirements = resources.JobRequirements(
        {resources.ResourceType.CPU: 4}, v100=1
    )
    task_requirements = requirements.task_requirements
    self.assertEqual(task_requirements[resources.ResourceType.CPU], 4)
    self.assertEqual(task_requirements[resources.ResourceType.V100], 1)
    self.assertEqual(requirements.replicas, 1)

  def test_resource_specified_twice(self):
    with self.assertRaises(ValueError):
      resources.JobRequirements({resources.ResourceType.CPU: 1}, cpu=2)

  def test_tpu_job(self):
    requirements = resources.JobRequirements(tpu_v3='4x4')
    self.assertEqual(requirements.accelerator, resources.ResourceType.TPU_V3)

  def test_replicas(self):
    requirements = resources.JobRequirements(replicas=2)
    self.assertEqual(requirements.replicas, 2)

    requirements = resources.JobRequirements(replicas=2, tpu_v3='4x4')
    self.assertEqual(requirements.replicas, 2)
    self.assertEqual(requirements.accelerator, resources.ResourceType.TPU_V3)
    assert requirements.topology is not None
    self.assertEqual(requirements.topology.name, '4x4')

    resources.JobRequirements(replicas=2, v100='4x2')
    resources.JobRequirements(v100='4x2')
    with self.assertRaises(ValueError):
      resources.JobRequirements(replicas=4, v100='4x2')

  def test_str(self):
    self.assertEqual(
        repr(
            resources.JobRequirements(
                cpu=1,
                location='lon_r7',
                service_tier=resources.ServiceTier.BATCH,
                replicas=2,
            )
        ),
        (
            "xm.JobRequirements(cpu=1.0, location='lon_r7',"
            ' service_tier=xm.ServiceTier.BATCH, replicas=2)'
        ),
    )

  def test_str_omits_empty_fields(self):
    self.assertEqual(
        repr(resources.JobRequirements(cpu=1)), 'xm.JobRequirements(cpu=1.0)'
    )

  def test_is_gpu_tpu_given_cpu(self):
    requirements = resources.JobRequirements(cpu=1, ram=4 * xm.GiB)
    self.assertNotIn(requirements.accelerator, xm.GpuType)
    self.assertNotIn(requirements.accelerator, xm.TpuType)

  def test_is_gpu_tpu_given_gpu(self):
    requirements = resources.JobRequirements(cpu=1, v100=4)
    self.assertIn(requirements.accelerator, xm.GpuType)
    self.assertNotIn(requirements.accelerator, xm.TpuType)

  def test_is_gpu_tpu_given_tpu(self):
    requirements = resources.JobRequirements(cpu=1, tpu_v2='2x2')
    self.assertNotIn(requirements.accelerator, xm.GpuType)
    self.assertIn(requirements.accelerator, xm.TpuType)


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
  absltest.main()
