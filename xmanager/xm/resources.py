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
"""Resources specification for use in the API.

Various classes defined to support resources specification for jobs.
"""

import enum
import functools
import operator
import re
from typing import Any, Dict, MutableMapping, Optional, Tuple, Union

from xmanager.xm import pattern_matching


class ResourceType(enum.Enum):
  """Type of a countable resource (e.g., CPU, memory, accelerators etc).

  We use a schema in which every particular accelerator has its own type. This
  way all countable resources required for a job could be represented by a
  simple dictionary.
  """

  # Amount of required CPU resources in vCPUs.
  CPU = 100002
  # Amount of required memory resources in bytes.
  MEMORY = 39
  RAM = 39
  # Amount of required disk resources in bytes.
  EPHEMERAL_STORAGE = 100003

  # GPUs

  P4 = 21
  T4 = 22
  P100 = 14
  V100 = 17
  A100 = 46

  # TPUs
  V2 = 3
  V3 = 16

  # TODO: do we need V2_DONUT and V3_DONUT?

  def __str__(self):
    return self._name_


class ResourceDict(MutableMapping):
  """Internal class to represent amount of countable resources.

  A mapping from ResourceType to amount of the resource combined with
  convenience methods. This class only tracks amounts of the resources, but not
  their topologies, locations or constraints.

  This class is rather generic and is designed be used internally as job
  requirements as well as in the executors. API users should not use it
  explicitly.

  Usage:
    # Construct (implicitly) from user code using JobRequirements:
    requirements = JobRequirements(cpu=0.5 * xm.vCPU, memory=2 * xm.GiB, v100=8)
    resources = requirements.task_requirements
    # Resources are available by their canonical names.
    assert(resources[ResourceType.V100], 8)
    # Print user-friendly representation:
    print(f'The task needs {resources}')
  """

  def __init__(self) -> None:
    self.__dict: Dict[ResourceType, float] = {}

  def __setitem__(self, key: ResourceType, value: float) -> None:
    self.__dict.__setitem__(key, value)

  def __getitem__(self, key: ResourceType) -> float:
    return self.__dict.__getitem__(key)

  def __delitem__(self, key: ResourceType) -> None:
    self.__dict.__delitem__(key)

  def __iter__(self):
    return self.__dict.__iter__()

  def __len__(self) -> int:
    return self.__dict.__len__()

  def __str__(self) -> str:
    """Returns user-readable text representation.

    Such as "V100: 8, CPU: 1.2, MEMORY: 5.4GiB".
    """
    # TODO: We do not aggregate memory yet, update this method to be more
    # user-friendly.
    return ', '.join(
        sorted([f'{key}: {value}' for (key, value) in self.items()]))


# TODO: Use centralized resource metadata.
_TPU_RESOURCES = (
    ResourceType.V2,
    ResourceType.V3,
)
_GPU_RESOURCES = (
    ResourceType.P4,
    ResourceType.T4,
    ResourceType.P100,
    ResourceType.V100,
    ResourceType.A100,
)


def resource_type_by_name(resource_name: str) -> ResourceType:
  """Returns a ResourceType corresponding to the given name.

  ResourceType keys are upper case, but we allow other case for the input.

  Args:
    resource_name: name of the resource type, arbitrary case.

  Returns:
    a ResourceType value corresponding to the given name.
  """
  return ResourceType[resource_name.upper()]


def is_gpu(resource_type: ResourceType) -> bool:
  return resource_type in _GPU_RESOURCES


def is_tpu(resource_type: ResourceType) -> bool:
  return resource_type in _TPU_RESOURCES


class InvalidTpuTopologyError(Exception):
  """An unrecognized TPU topology has been provided."""


class Topology:
  """Accelerator topology configuration.

  Describes accelerator interconnection. For example could be a TPU topology or
  several GPUs connected with NVLink. Topologies have a form of 'NxM_suffix'
  where N & M are the number of accelerators across the dimension and suffix
  corresponds to a specific interconnect type. Number of dimensions may vary.

  Examples of valid topologies:
    '1' - a single device.
    '4' - 4 GPUs on one host.
    '4x4' - A 4x4 TPU grid.
  """

  def __init__(self, name: str) -> None:
    if not re.fullmatch('([\\d]+x?)+(_(un)?twisted)?', name):
      raise InvalidTpuTopologyError(f'Invalid TPU topology: {name}.')

    self._name = name

    dimensions_str = name.split('_')[0]
    self._dimensions = list(map(int, dimensions_str.split('x')))

  @property
  def chip_count(self) -> int:
    """Returns the number of chips of the TPU topology."""
    return functools.reduce(operator.mul, self._dimensions)

  @property
  def name(self) -> str:
    """Returns the topology as a string."""
    return self._name


ResourceQuantity = Union[int, float, str, Topology]


def _parse_resource_quantity(
    value: ResourceQuantity) -> Tuple[float, Optional[Topology]]:
  """Parses a string representation of a resource quantity."""

  def parse_string(value: str):
    if 'x' in value:
      topology = Topology(value)
      return topology.chip_count, topology
    else:
      # TODO: Parse SI suffixes, like GiB.
      return float(value), None

  def parse_topology(topology: Topology):
    return topology.chip_count, topology

  def parse_number(value: Any):
    return float(value), None

  return pattern_matching.match(parse_string, parse_topology, parse_number)(
      value)


class JobRequirements:
  """Describes the resource requirements of a Job.

  Attribures:
    task_requirements: Amount of resources needed for a single task within a
      job.
    accelerator: The accelearator the jobs uses, if there is one. Jobs using
      multiple accelerators are not supported because different kinds of
      accelerators are usually not installed on the same host.
    topology: Accelerator topology, if an accelerator is used.
  """

  task_requirements: ResourceDict
  accelerator: Optional[ResourceType]
  topology: Optional[Topology]

  def __init__(self, **resources: ResourceQuantity) -> None:
    """Define a set of resources.

    Args:
      **resources: resource amounts, for example v100=2 or ram=1 * xm.GiB.
    """
    self.task_requirements = ResourceDict()
    self.accelerator = None
    self.topology = None
    # TODO: Consider checking .accelerator type instead.
    self.is_tpu_job = False
    self.is_gpu_job = False

    for resource_name, value in resources.items():
      scalar, topology = _parse_resource_quantity(value)
      resource = resource_type_by_name(resource_name)

      if is_tpu(resource) or is_gpu(resource):
        if self.accelerator is not None:
          raise ValueError('Accelerator already set.')
        self.accelerator = resource
        self.topology = topology or Topology(f'{scalar:g}')
      elif topology is not None:
        raise ValueError(
            f'A topology specified for non accelerator resource {resource}.')

      if is_tpu(resource):
        self.is_tpu_job = True
      elif is_gpu(resource):
        self.is_gpu_job = True

      self.task_requirements[resource] = scalar
