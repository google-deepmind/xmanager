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

import builtins
import enum
import functools
import itertools
import math
import operator
import re
from typing import Any, Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Tuple, TypeVar, Union, cast

import immutabledict


class _CaseInsensitiveResourceTypeMeta(enum.EnumMeta):
  """Metaclass which allows case-insensitive enum lookup.

  Enum keys are upper case, but we allow other cases for the input. For
  example existing flags and JobRequirements use lower case for resource names.
  """

  def __getitem__(cls, resource_name: str) -> 'ResourceType':
    try:
      return super().__getitem__(resource_name.upper())  # pyrefly: ignore[bad-argument-type]
    except KeyError:
      raise KeyError(f'Unknown {cls.__name__} {resource_name!r}')  # pylint: disable=raise-missing-from


class Architecture(enum.Enum):
  """CPU architecture types for resources."""

  HASWELL = 1
  ARM = 2


class ResourceType(enum.Enum, metaclass=_CaseInsensitiveResourceTypeMeta):
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
  LOCAL_GPU = 100006
  P4 = 21
  T4 = 22
  L4_24TH = 68
  P100 = 14
  V100 = 17
  A100 = 46
  A100_80GIB = 66
  H100 = 70
  H200 = 86
  B200 = 87
  B300 = 112
  GB200 = 89
  GB300 = 100
  B40_48TH = 93

  # TPUs
  TPU_V2 = 3
  TPU_V3 = 16
  TPU_V4 = 34
  TPU_V5E = 60
  TPU_V5P = 59
  TPU_V6E = 63
  TPU_V7X = 92

  # TODO: do we need V2_DONUT and V3_DONUT?

  def __str__(self):
    return self._name_


class _CaseInsensitiveServiceTierMeta(enum.EnumMeta):
  """Metaclass which allows case-insensitive enum lookup.

  Enum keys are upper case, but we allow other cases for the input. For
  example existing flags and JobRequirements use lower case for resource names.
  """

  def __getitem__(cls, resource_name: str) -> 'ServiceTier':
    try:
      return super().__getitem__(resource_name.upper())  # pyrefly: ignore[bad-argument-type]
    except KeyError:
      raise KeyError(f'Unknown {cls.__name__} {resource_name!r}')  # pylint: disable=raise-missing-from


class ServiceTier(enum.Enum, metaclass=_CaseInsensitiveServiceTierMeta):
  """The job availability guarantees which underlying platform should provide.

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


_DEFAULT_SERVICE_TIER = ServiceTier.PROD


def _enum_subset(class_name: str, values: Iterable[ResourceType]) -> Any:
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
            f"type object '{cls.__name__}' has no attribute '{item}'"
        )
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
        ResourceType.TPU_V4,
        ResourceType.TPU_V5E,
        ResourceType.TPU_V5P,
        ResourceType.TPU_V6E,
        ResourceType.TPU_V7X,
    ],
)


GpuType = _enum_subset(
    'GpuType',
    [
        # LOCAL_GPU is missing as only specific GPU types should be added.
        ResourceType.P4,
        ResourceType.T4,
        ResourceType.L4_24TH,
        ResourceType.P100,
        ResourceType.V100,
        ResourceType.A100,
        ResourceType.A100_80GIB,
        ResourceType.H100,
        ResourceType.H200,
        ResourceType.B200,
        ResourceType.B300,
        ResourceType.GB200,
        ResourceType.GB300,
        ResourceType.B40_48TH,
    ],
)


AcceleratorType = _enum_subset(
    'AcceleratorType',
    [
        ResourceType.LOCAL_GPU,
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
        sorted([f'{key}: {value}' for (key, value) in self.items()])
    )

  def __add__(self: 'ResourceDict', rhs: 'ResourceDict') -> 'ResourceDict':
    """Returns a sum of two ResourceDicts."""
    result = ResourceDict()
    for key in [*self.keys(), *rhs.keys()]:
      result[key] = self.get(key, 0) + rhs.get(key, 0)
    return result

  def __mul__(self: 'ResourceDict', rhs: float) -> 'ResourceDict':
    """Returns the multiplication of a ResourceDict with a scalar."""
    result = ResourceDict()
    for key, value in self.items():
      result[key] = value * rhs
    return result

  def __rmul__(self: 'ResourceDict', rhs: float) -> 'ResourceDict':
    """Returns the multiplication of a ResourceDict with a scalar."""
    return self * rhs


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
    '4x4' - a 4x4 TPU grid.
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

  def __eq__(self, other: 'Topology') -> bool:  # pyrefly: ignore[bad-override]
    if not isinstance(other, Topology):
      return False
    return self.name == other.name

  def __hash__(self) -> int:
    return hash(self.name)


ResourceQuantity = Union[int, float, str, Topology]
_ResourceTypeOrStr = TypeVar(
    '_ResourceTypeOrStr', bound=Union[ResourceType, str]
)


def _parse_resource_quantity(
    resource_name: str, value: ResourceQuantity
) -> tuple[float, Topology | None, int | None]:
  """Parses a string representation of a resource quantity.

  Accelerator quantity strings may have a `*N` suffix specifying the number of
  slices, e.g. '4x18*3' means 3 slices of topology 4x18.

  Args:
    resource_name: The name of the resource, used in error messages.
    value: The resource quantity to parse.

  Returns:
    A tuple of (scalar, topology, num_slices). The scalar and topology describe
    a single slice. num_slices is None if no `*N` suffix was given.
  """

  def parse_string(value: str):
    value, sep, num_slices_str = value.partition('*')
    num_slices = int(num_slices_str) if sep else None
    if 'x' in value:
      topology = Topology(value)
      return topology.chip_count, topology, num_slices
    else:
      # TODO: Parse SI suffixes, like GiB.
      return float(value), None, num_slices

  try:
    match value:
      case builtins.str() as str_value:
        return parse_string(str_value)
      case Topology():
        topology = cast(Topology, value)  # needed to work around a pytype bug
        return topology.chip_count, topology, None
      case _:
        return float(value), None, None
  except Exception as e:
    raise ValueError(
        f"Couldn't parse resource quantity for {resource_name}. "
        f'{value!r} was given.'
    ) from e


def _parse_replicas(value: ResourceQuantity) -> int:
  """Parses a `replicas` resource quantity into a positive integer."""
  if isinstance(value, Topology):
    raise ValueError(f'replicas must be a positive integer, got {value!r}')
  try:
    scalar = float(value)
  except ValueError as e:
    raise ValueError(
        f'replicas must be a positive integer, got {value!r}'
    ) from e
  if not scalar.is_integer() or scalar < 1:
    raise ValueError(f'replicas must be a positive integer, got {value!r}')
  return int(scalar)


class JobRequirements:
  # pyformat: disable
  """Describes the resource requirements of a Job.

  Attributes:
    task_requirements: Amount of resources needed for a single task within a
      job.
    accelerator: The accelerator the jobs uses, if there is one. Jobs using
      multiple accelerators are not supported because different kinds of
      accelerators are usually not installed on the same host.
    architecture: The architecture of the CPU the job should run on.
    topology: Accelerator topology, if an accelerator is used.
    location: Place where the job should run. For example a cluster name or a
      Borg cell.
    service_tier: A service tier at which the job should run.
    replicas: Number of identical tasks to run within a job
    num_slices: Number of independent accelerator slices within the job. The
      accelerator topology describes a single slice. Defaults to 1. Not yet
      supported for TPUs.
  """
  # pyformat:enable

  task_requirements: ResourceDict
  accelerator: Optional[ResourceType]
  architecture: Optional[Architecture]
  topology: Optional[Topology]

  location: Optional[str]
  _service_tier: ServiceTier
  _num_slices: int

  def _validate_architecture_and_accelerator(self):
    """Validates that the architecture is compatible with the accelerator."""
    if self.architecture is None or self.accelerator is None:
      return
    try:
      accelerator_architecture = self.accelerator.architecture()
    except ValueError:
      return
    if self.architecture != accelerator_architecture:
      raise ValueError(
          f'Accelerator {self.accelerator} requires architecture'
          f' {accelerator_architecture}, but {self.architecture} was specified.'
      )

  def _validate_accelerator_topology(self):
    """Validates that the topology is compatible with the accelerator."""

  def _validate_num_slices(
      self, num_slices: int | None, accelerator_num_slices_suffix: int | None
  ) -> int:
    """Validates num_slices and returns the resolved value.

    Args:
      num_slices: The `num_slices` argument, if specified.
      accelerator_num_slices_suffix: The `*N` suffix on the accelerator, if
        specified.

    Returns:
      The number of slices, 1 if not specified.
    """
    if accelerator_num_slices_suffix is not None:
      if num_slices is not None and num_slices != accelerator_num_slices_suffix:
        raise ValueError(
            f'Specified num_slices={num_slices} which does not match'
            f' {self.accelerator} suffix ({accelerator_num_slices_suffix}'
            ' slices).'
        )
      num_slices = accelerator_num_slices_suffix
    if num_slices is None:
      return 1
    if num_slices < 1:
      raise ValueError(f'num_slices must be positive, got {num_slices}')
    if num_slices > 1 and self.accelerator in TpuType:
      raise ValueError(
          f'num_slices is not supported for TPUs, got {num_slices} for'
          f' {self.accelerator}.'
      )
    return num_slices

  def __init__(
      self,
      resources: Mapping[
          _ResourceTypeOrStr, ResourceQuantity
      ] = immutabledict.immutabledict(),
      *,
      architecture: Optional[Architecture] = None,
      location: Optional[str] = None,
      replicas: Optional[int] = None,
      service_tier: Optional[ServiceTier] = None,
      num_slices: int | None = None,
      **kw_resources: ResourceQuantity,
  ) -> None:
    # pyformat: disable
    """Define a set of resources.

    Args:
      resources: resource amounts as a dictionary, for example
        {xm.ResourceType.V100: 2}.
      architecture: The architecture of the CPU the job should run on. If 
        not specified, the default architecture for the resource will be used.
      location: Place where the job should run. For example a cluster name or a
        Borg cell.
      replicas: Number of identical tasks to run within a job. 1 by default.
      service_tier: A service tier at which the job should run.
      num_slices: Number of independent accelerator slices within the job. The
        accelerator topology describes a single slice. Can alternatively be
        specified as a `*N` suffix on the accelerator, e.g. `gb300='4x18*3'`.
        1 by default.
      **kw_resources: resource amounts as a kwargs, for example `v100=2` or
        `ram=1 * xm.GiB`. See xm.ResourceType enum for the list of supported
        types and aliases.

    Raises:
      ValueError:
        If several accelerator resources are supplied (i.e. GPU and TPU).
        If the same resource is passed in a `resources` dictionary and as
          a command line argument.
        If topology is supplied for a non accelerator resource.
        If `num_slices` is specified both as an argument and as a `*N` suffix
          with different values, or as a suffix on a non accelerator resource.
        If `num_slices` > 1 is specified for a TPU.
        If `replicas` is not consistent with `num_slices`.
    """
    # pyformat: enable
    self.location = location
    self._service_tier = service_tier or _DEFAULT_SERVICE_TIER

    self.task_requirements = ResourceDict()
    self.accelerator = None
    self.topology = None

    accelerator_num_slices_suffix = None
    for resource_name, value in itertools.chain(
        resources.items(), kw_resources.items()
    ):
      if resource_name == 'replicas':
        if replicas is not None:
          raise ValueError('replicas has been specified twice.')
        replicas = _parse_replicas(value)
        continue

      scalar, topology, resource_num_slices = _parse_resource_quantity(resource_name, value)  # pylint: disable=unpacking-non-sequence  # pyrefly: ignore[bad-argument-type]
      match resource_name:
        case builtins.str() as r:
          resource = ResourceType[r]
        case ResourceType():
          resource = resource_name
        case _:
          raise TypeError(f'Unsupported resource: {resource_name!r}')

      if resource in AcceleratorType:
        if self.accelerator is not None:
          raise ValueError('Accelerator already set.')
        self.accelerator = resource
        self.topology = topology or Topology(f'{scalar:g}')
        self._validate_accelerator_topology()
        accelerator_num_slices_suffix = resource_num_slices
      elif topology is not None:
        raise ValueError(
            f'A topology specified for non accelerator resource {resource}.'
        )
      elif resource_num_slices is not None:
        raise ValueError(
            '"*" operator for num_slices specified on non-accelerator resource'
            f' {resource}.'
        )

      if resource in self.task_requirements:
        raise ValueError(f'{resource} has been specified twice.')
      self.task_requirements[resource] = scalar

    self._num_slices = self._validate_num_slices(
        num_slices, accelerator_num_slices_suffix
    )

    if (
        self.accelerator in GpuType
        and self.topology
        and (len(self.topology.dimensions) > 1 or self._num_slices > 1)
        and self.accelerator != ResourceType.B40_48TH
    ):
      # The topology describes a single slice, and each slice is an identical
      # copy of it.
      inferred_replicas = (
          math.prod(self.topology.dimensions[1:]) * self._num_slices
      )
      if replicas is not None and replicas != inferred_replicas:
        raise ValueError(
            f'For multihost GPUs with topology {self.topology} and num_slices'
            f' {self._num_slices}, replicas should be either None or'
            f' {inferred_replicas}. Got: {replicas}'
        )
      replicas = inferred_replicas

    self.replicas = replicas or 1

    if architecture is None and self.accelerator is not None:
      try:
        architecture = self.accelerator.architecture()
      except ValueError:
        pass
    self.architecture = architecture
    self._validate_architecture_and_accelerator()

  @property
  def service_tier(self):
    return self._service_tier

  @service_tier.setter
  def service_tier(self, new_service_tier):
    self._service_tier = new_service_tier

  @property
  def num_slices(self) -> int:
    return self._num_slices

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
    if self.architecture:
      args.append(f'architecture={self.architecture}')
    if self.service_tier != _DEFAULT_SERVICE_TIER:
      args.append(f'service_tier=xm.{self.service_tier}')
    if self.replicas != 1:
      args.append(f'replicas={self.replicas}')
    if self.num_slices != 1:
      args.append(f'num_slices={self.num_slices}')

    return f'xm.JobRequirements({", ".join(args)})'

  def merge(self, other: 'JobRequirements') -> 'JobRequirements':
    """Merges another JobRequirements into self, returning a new instance.

    Semantics: Override (other values overwrite self values if set/non-default).
    For task_requirements, keys in other overwrite keys in self.
    If other specifies an accelerator, self's accelerator is removed first.

    Args:
      other: The other JobRequirements to merge into self.

    Returns:
      A new JobRequirements instance representing the merged requirements.
    """
    merged_resources = dict(self.task_requirements)
    if other.accelerator is not None and self.accelerator is not None:
      if self.accelerator in merged_resources:
        del merged_resources[self.accelerator]
    merged_resources.update(other.task_requirements)

    kwargs = {}
    if other.location is not None:
      kwargs['location'] = other.location
    else:
      kwargs['location'] = self.location

    if other.architecture is not None:
      kwargs['architecture'] = other.architecture
    else:
      kwargs['architecture'] = self.architecture

    if other.replicas != 1:
      kwargs['replicas'] = other.replicas
    elif self.replicas != 1:
      kwargs['replicas'] = self.replicas
    # Otherwise leave replicas unset so it can be inferred (e.g. from
    # num_slices) instead of being forced to 1.

    if other.num_slices != 1:
      kwargs['num_slices'] = other.num_slices
    else:
      kwargs['num_slices'] = self.num_slices

    return JobRequirements(merged_resources, **kwargs)

  def __eq__(self, other: 'JobRequirements') -> bool:  # pyrefly: ignore[bad-override]
    if not isinstance(other, JobRequirements):
      return False
    return (
        self.task_requirements == other.task_requirements
        and self.accelerator == other.accelerator
        and self.topology == other.topology
        and self.location == other.location
        and self.service_tier == other.service_tier
        and self.replicas == other.replicas
        and self.num_slices == other.num_slices
    )
