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
"""XManager client API.

Provides XManager public API for configuring and launching experiments.
"""

from xmanager.xm import job_operators
from xmanager.xm.compute_units import *
from xmanager.xm.core import AuxiliaryUnitJob
from xmanager.xm.core import AuxiliaryUnitRole
from xmanager.xm.core import Experiment
from xmanager.xm.core import ExperimentUnit
from xmanager.xm.core import ExperimentUnitError
from xmanager.xm.core import ExperimentUnitFailedError
from xmanager.xm.core import ExperimentUnitNotCompletedError
from xmanager.xm.core import ExperimentUnitRole
from xmanager.xm.core import ExperimentUnitStatus
from xmanager.xm.core import Importance
from xmanager.xm.core import NotFoundError
from xmanager.xm.core import WorkUnit
from xmanager.xm.core import WorkUnitCompletedAwaitable
from xmanager.xm.core import WorkUnitRole
from xmanager.xm.executables import BazelBinary
from xmanager.xm.executables import BazelContainer
from xmanager.xm.executables import Binary
from xmanager.xm.executables import BinaryDependency
from xmanager.xm.executables import CommandList
from xmanager.xm.executables import Container
from xmanager.xm.executables import Dockerfile
from xmanager.xm.executables import ModuleName
from xmanager.xm.executables import PythonContainer
from xmanager.xm.job_blocks import Constraint
from xmanager.xm.job_blocks import Executable
from xmanager.xm.job_blocks import ExecutableSpec
from xmanager.xm.job_blocks import Executor
from xmanager.xm.job_blocks import ExecutorSpec
from xmanager.xm.job_blocks import Job
from xmanager.xm.job_blocks import JobGeneratorType
from xmanager.xm.job_blocks import JobGroup
from xmanager.xm.job_blocks import JobType
from xmanager.xm.job_blocks import merge_args
from xmanager.xm.job_blocks import Packageable
from xmanager.xm.job_blocks import SequentialArgs
from xmanager.xm.job_blocks import UserArgs
from xmanager.xm.metadata_context import ContextAnnotations
from xmanager.xm.metadata_context import MetadataContext
from xmanager.xm.packagables import bazel_binary
from xmanager.xm.packagables import bazel_container
from xmanager.xm.packagables import binary
from xmanager.xm.packagables import container
from xmanager.xm.packagables import dockerfile_container
from xmanager.xm.packagables import python_container
from xmanager.xm.resources import GpuType
from xmanager.xm.resources import InvalidTpuTopologyError
from xmanager.xm.resources import JobRequirements
from xmanager.xm.resources import ResourceDict
from xmanager.xm.resources import ResourceQuantity
from xmanager.xm.resources import ResourceType
from xmanager.xm.resources import ServiceTier
from xmanager.xm.resources import Topology
from xmanager.xm.resources import TpuType
from xmanager.xm.utils import run_in_asyncio_loop
from xmanager.xm.utils import ShellSafeArg
