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
import itertools
import operator
import re
from typing import Any, Iterable, Iterator, Dict, Mapping, MutableMapping, Optional, Tuple, Union

import immutabledict
from xmanager.xm import pattern_matching as pm


class _CaseInsensetiveEnumMeta(enum.EnumMeta):
  """Metaclass which allows case-insensetive enum lookup.

  Enum keys are upper case, but we allow other cases for the input. For
  example existing flags and JobRequirements use lower case for resource names.
  """

  def __getitem__(cls, resource_name: str) -> 'ResourceType':
    try:
      return super().__getitem__(resource_name.upper())
    except KeyError:
      raise KeyError(f'Unknown {cls.__name__} {resource_name!r}')


class ResourceType(enum.Enum, metaclass=_CaseInsensetiveEnumMeta):
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
  DISK = 100003

  # GPUs

  P4 = 21
  T4 = 22
  P100 = 14
  V100 = 17
  A100 = 46

  # TPUs
  TPU_V2 = 3
  TPU_V3 = 16

  # TODO: do we need V2_DONUT and V3_DONUT?

  def __str__(self):
    return self._name_


class ServiceTier(enum.Enum, metaclass=_CaseInsensetiveEnumMeta):
  """The job availability guarantees which underlying platfrom should provide.

  Most cloud platforms offer a selection of availability/price tradeoff options.
  Usually there are at least "Take my money, this workload is important" and
  "Buy excess compute for cheap" offerings. This enum provides a classification
  of such offerings and allows matching comparable (but not necessary identical)
  options from different runtimes.
  """

  # Highly available tier. The job is expected to be scheduled fast once sent to
  # the cloud. Recommended tier for multi-job work units as lower tiers may lead
  # to partially-scheduled work units.
  PROD = 200
  # A cheaper tier with guaranteed average throughput. Jobs may spend hours
  # awaiting scheduling by the cloud and can be preempted.
  BATCH = 100
  # The cheapest tier. No guarantees, but it often works.
  FREEBIE = 25


def _enum_subset(class_name: str, values: Iterable[ResourceType]) -> type:  # pylint: disable=g-bare-generic
  """Returns an enum subset class.

  The class is syntactically equivalent to an enum with the given resource
  types. But the concrete constants are the same as in the ResourceType enum,
  making all equivalence comparisons work correctly. Additionally operator `in`
  is supported for checking if a resource belongs to the subset.

  Args:
    class_name: Class name of the subset enum.
    values: A list of resources that belong to the subset.
  """
  values = set(values)

  class EnumSubsetMetaclass(type):  # pylint: disable=g-bare-generic
    """Metaclass which implements enum subset operations."""

    def __new__(
        cls,
        name: str,
        bases: Tuple[type],  # pylint: disable=g-bare-generic
        dct: Dict[str, Any],
    ) -> type:  # pylint: disable=g-bare-generic
      # Add constants to the class dict.
      for name, member in ResourceType.__members__.items():
        if member in values:
          dct[name] = member

      return super().__new__(cls, class_name, bases, dct)

    def __getitem__(cls, item: str) -> ResourceType:
      result = ResourceType[item]
      if result not in cls:  # pylint: disable=unsupported-membership-test
        raise AttributeError(
            f"type object '{cls.__name__}' has no attribute '{item}'")
      return result

    def __iter__(cls) -> Iterator[ResourceType]:
      return iter(values)

    def contains(cls, value: ResourceType) -> bool:
      return value in values

  class EnumSubset(metaclass=EnumSubsetMetaclass):

    def __new__(cls, value: int) -> ResourceType:
      resource = ResourceType(value)
      if resource not in cls:
        raise ValueError(f'{value} is not a valid {cls.__name__}')
      return resource

  return EnumSubset


# TODO: Use centralized resource metadata.
TpuType = _enum_subset(
    'TpuType',
    [
        ResourceType.TPU_V2,
        ResourceType.TPU_V3,
    ],
)


GpuType = _enum_subset(
    'GpuType',
    [
        ResourceType.P4,
        ResourceType.T4,
        ResourceType.P100,
        ResourceType.V100,
        ResourceType.A100,
    ],
)


_AcceleratorType = _enum_subset(
    '_AcceleratorType',
    [
        *list(TpuType),
        *list(GpuType),
    ],
)


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
    self.dimensions = list(map(int, dimensions_str.split('x')))

  @property
  def chip_count(self) -> int:
    """Returns the number of chips of the TPU topology."""
    return functools.reduce(operator.mul, self.dimensions)

  @property
  def name(self) -> str:
    """Returns the topology as a string."""
    return self._name

  def __repr__(self) -> str:
    return f'xm.Topology({self.name!r})'

  def __eq__(self, other: 'Topology') -> bool:
    return self.name == other.name

  def __hash__(self) -> int:
    return hash(self.name)


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

  return pm.match(parse_string, parse_topology, parse_number)(value)


class JobRequirements:
  """Describes the resource requirements of a Job.

  Attribures:
    task_requirements: Amount of resources needed for a single task within a
      job.
    accelerator: The accelearator the jobs uses, if there is one. Jobs using
      multiple accelerators are not supported because different kinds of
      accelerators are usually not installed on the same host.
    topology: Accelerator topology, if an accelerator is used.
    location: Place where the job should run. For example a cluster name or
      a Borg cell.
    service_tier: A service tier at which the job should run.
  """

  task_requirements: ResourceDict
  accelerator: Optional[ResourceType]
  topology: Optional[Topology]

  location: Optional[str]
  service_tier: ServiceTier

  def __init__(
      self,
      resources: Mapping[Union[ResourceType, str],
                         ResourceQuantity] = immutabledict.immutabledict(),
      *,
      location: Optional[str] = None,
      replicas: Optional[int] = None,
      service_tier: Optional[ServiceTier] = None,
      **kw_resources: ResourceQuantity) -> None:
    """Define a set of resources.

    Args:
      resources: resource amounts as a dictionary,
        for example {xm.ResourceType.V100: 2}.
      location: Place where the job should run. For example a cluster name or a
        Borg cell.
      replicas: Number of identical tasks to run winthin a job. 1 by default.
      service_tier: A service tier at which the job should run.
      **kw_resources: resource amounts as a kwargs, for example `v100=2` or
        `ram=1 * xm.GiB`. See xm.ResourceType enum for the list of supported
        types and aliases.

    Raises:
      ValueError:
        If several accelerator resources are supplied (i.e. GPU and TPU).
        If the same resource is passed in a `resources` dictionary and as
          a command line argument.
        If topology is supplied for a non acceelerator resource.
    """
    self.location = location
    self.service_tier = service_tier or ServiceTier.PROD

    self.task_requirements = ResourceDict()
    self.accelerator = None
    self.topology = None

    for resource_name, value in itertools.chain(resources.items(),
                                                kw_resources.items()):
      scalar, topology = _parse_resource_quantity(value)
      resource = pm.match(
          pm.Case([str], lambda r: ResourceType[r]),
          pm.Case([ResourceType], lambda r: r))(
              resource_name)

      if resource in _AcceleratorType:
        if self.accelerator is not None:
          raise ValueError('Accelerator already set.')
        self.accelerator = resource
        self.topology = topology or Topology(f'{scalar:g}')
      elif topology is not None:
        raise ValueError(
            f'A topology specified for non accelerator resource {resource}.')

      if resource in self.task_requirements:
        raise ValueError(f'{resource} has been specified twice.')
      self.task_requirements[resource] = scalar

    if (self.accelerator in GpuType and self.topology and
        len(self.topology.dimensions) == 2):
      if replicas is not None and replicas != self.topology.dimensions[1]:
        raise ValueError(
            f'For multihost GPUs with topology {self.topology}, replicas should'
            f'be either None or {self.topology.dimensions[1]}. Found: '
            f'{replicas}')
      replicas = self.topology.dimensions[1]

    self.replicas = replicas or 1
    self._validate_replicas()

  def _validate_replicas(self) -> None:
    """Raises ValueError if replication is not supported."""
    if self.replicas > 1 and self.accelerator in TpuType:
      raise ValueError(
          f'Replicated jobs are not supported for {self.accelerator}.')

  def __repr__(self) -> str:
    """Returns string representation of the requirements."""
    args = []

    for resource, value in self.task_requirements.items():
      if resource in TpuType:
        args.append(f'{resource.name.lower()}={self.topology!r}')
      else:
        args.append(f'{resource.name.lower()}={value!r}')

    if self.location:
      args.append(f'location={self.location!r}')
    if self.service_tier != ServiceTier.PROD:
      args.append(f'service_tier=xm.{self.service_tier}')
    if self.replicas != 1:
      args.append(f'replicas={self.replicas}')

    return f'xm.JobRequirements({", ".join(args)})'
