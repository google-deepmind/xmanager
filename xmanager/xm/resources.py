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
from typing import Dict, MutableMapping


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


def is_gpu(resource_type: ResourceType) -> bool:
  return resource_type in _GPU_RESOURCES


def is_tpu(resource_type: ResourceType) -> bool:
  return resource_type in _TPU_RESOURCES


def resource_type_by_name(resource_name: str) -> ResourceType:
  """Returns a ResourceType corresponding to the given name.

  ResourceType keys are upper case, but we allow other case for the input.

  Args:
    resource_name: name of the resource type, arbitrary case.

  Returns:
    a ResourceType value corresponding to the given name.
  """
  return ResourceType[resource_name.upper()]


class JobRequirements:
  """Describes the resource requirements of a Job."""

  def __init__(self, **resources: float) -> None:
    """Define a set of resources.

    Args:
      **resources: resource amounts, for example v100=2 or ram=1 * xm.GiB.
    """
    self.is_tpu_job = False
    self.is_gpu_job = False
    self.task_requirements = ResourceDict()

    for resource_name, value in resources.items():
      resource = resource_type_by_name(resource_name)
      if is_tpu(resource):
        self.is_tpu_job = True
      if is_gpu(resource):
        self.is_gpu_job = True

      self.task_requirements[resource] = value
