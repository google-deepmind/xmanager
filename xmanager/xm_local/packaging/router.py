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
"""Methods for routing packageables to appropriate packagers."""

from typing import Any

from xmanager import xm
from xmanager.xm import pattern_matching
from xmanager.xm_local import executors
from xmanager.xm_local.packaging import cloud as cloud_packaging
from xmanager.xm_local.packaging import local as local_packaging


def _visit_caip_spec(packageable: xm.Packageable, _: executors.CaipSpec):
  return cloud_packaging.package_cloud_executable(packageable,
                                                  packageable.executable_spec)


def _visit_local_spec(packageable: xm.Packageable, _: executors.LocalSpec):
  return local_packaging.package_for_local_executor(packageable,
                                                    packageable.executable_spec)


def _visit_kubernetes_spec(packageable: xm.Packageable,
                           _: executors.KubernetesSpec):
  return cloud_packaging.package_cloud_executable(packageable,
                                                  packageable.executable_spec)


def _throw_on_unknown_executor(packageable: xm.Packageable, executor: Any):
  raise TypeError(f'Unsupported executor specification: {executor!r}. '
                  f'Packageable: {packageable!r}')


_PACKAGING_ROUTER = pattern_matching.match(_visit_caip_spec, _visit_local_spec,
                                           _visit_kubernetes_spec,
                                           _throw_on_unknown_executor)


def package(packageable: xm.Packageable) -> xm.Executable:
  """Routes a packageable to an appropriate packaging mechanism."""
  return _PACKAGING_ROUTER(packageable, packageable.executor_spec)
