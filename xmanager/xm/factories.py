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
"""Convenience methods for constructing core objects."""

from typing import Callable, Type

from xmanager.xm import core
from xmanager.xm import executables


# TODO: Provide proper typing for autocompletion.
def create_packageable_factory(
    ctor: Type[core.ExecutableSpec]) -> Callable[..., core.Packageable]:
  """Creates a factory function based on a particular executable spec."""

  def packageable_factory(executor_spec: core.ExecutorSpec, *args,
                          **kwargs) -> core.Packageable:
    pkg_args = kwargs.pop('args', [])
    pkg_env_vars = kwargs.pop('env_vars', {})
    return core.Packageable(
        executable_spec=ctor(*args, **kwargs),
        executor_spec=executor_spec,
        args=pkg_args,
        env_vars=pkg_env_vars,
    )

  return packageable_factory


binary = create_packageable_factory(executables.Binary)
bazel_binary = create_packageable_factory(executables.BazelBinary)
container = create_packageable_factory(executables.Container)
bazel_container = create_packageable_factory(executables.BazelContainer)
python_container = create_packageable_factory(executables.PythonContainer)
