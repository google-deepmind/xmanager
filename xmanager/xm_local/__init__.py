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
"""Implementation of the XManager Launch API within the local scheduler."""

import typing

# Ensure xm flags are available.
from xmanager import xm_flags as _xm_flags
from xmanager.module_lazy_loader import module_lazy_loader as _module_lazy_loader

# xmanager.xm_local APIs.
# To add an API, add a new entry to this list, and also
# import the API under the 'typing.TYPE_CHECKING' block below. If not already
# there, add the API's parent module to the list of private submodules as well.
_apis = [
    _module_lazy_loader.XManagerAPI(
        module="xmanager.cloud.auth",
        alias="auth",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.cloud.vertex",
        alias="vertex",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="Caip",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="CaipSpec",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="DockerOptions",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="Kubernetes",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="KubernetesSpec",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="Local",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="LocalSpec",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="TensorboardCapability",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="TpuCapability",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="Vertex",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        symbol="VertexSpec",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.experiment",
        symbol="create_experiment",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.experiment",
        symbol="get_experiment",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.experiment",
        symbol="list_experiments",
    ),
]

# Private submodules from this package. Exposed so that runtime behavior
# matches what is visible to the type checker, but these should not be used
# by end users.
_private_submodules = [
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.executors",
        alias="_executors",
    ),
    _module_lazy_loader.XManagerAPI(
        module="xmanager.xm_local.experiment",
        alias="_experiment",
    ),
]

_exports = _apis + _private_submodules

_lazy_loader = _module_lazy_loader.XManagerLazyLoader(__name__, _exports)

__all__ = _lazy_loader.get_module_all()

__dir__ = _lazy_loader.get_module_dir()

__getattr__ = _lazy_loader.get_module_getattr()


# Eagerly import exported symbols only when run under static analysis so that
# code references work.
if typing.TYPE_CHECKING:
  # pylint: disable=g-bad-import-order
  from xmanager.cloud import auth
  from xmanager.cloud import vertex
  from xmanager.xm_local import experiment as _experiment
  from xmanager.xm_local import executors as _executors

  Caip = _executors.Caip
  CaipSpec = _executors.CaipSpec
  DockerOptions = _executors.DockerOptions
  Kubernetes = _executors.Kubernetes
  KubernetesSpec = _executors.KubernetesSpec
  Local = _executors.Local
  LocalSpec = _executors.LocalSpec
  TensorboardCapability = _executors.TensorboardCapability
  TpuCapability = _executors.TpuCapability
  Vertex = _executors.Vertex
  VertexSpec = _executors.VertexSpec

  create_experiment = _experiment.create_experiment
  get_experiment = _experiment.get_experiment
  list_experiments = _experiment.list_experiments
del typing
