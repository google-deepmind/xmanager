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
"""Packaging for local executions."""

import os
from typing import Any

from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.cloud import build_image
from xmanager.cloud import docker_lib
from xmanager.docker import docker_adapter
from xmanager.xm import executables
from xmanager.xm import pattern_matching
from xmanager.xm_local import executables as local_executables


def _package_container(packageable: xm.Packageable,
                       container: executables.Container) -> xm.Executable:
  """Packages a container for local execution."""
  instance = docker_adapter.instance()
  image_id = None
  if os.path.exists(container.image_path):
    image_id = instance.load_image(container.image_path)
  elif instance.is_registry_label(container.image_path):
    image_id = instance.pull_image(container.image_path)
  if image_id is not None:
    return local_executables.LoadedContainerImage(
        name=packageable.executable_spec.name,
        image_id=image_id,
        args=packageable.args,
        env_vars=packageable.env_vars,
    )
  else:
    raise ValueError(
        f'{container.image_path} is found neither locally nor remotely')


def _package_binary(packageable: xm.Packageable, binary: executables.Binary):
  if not os.path.exists(binary.path):
    raise ValueError(f'{binary.path} does not exist on this machine')
  return local_executables.LocalBinary(
      name=packageable.executable_spec.name,
      path=binary.path,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_dockerfile(packageable: xm.Packageable,
                        dockerfile: executables.Dockerfile):
  image_id = docker_lib.build_docker_image(dockerfile.name, dockerfile.path)
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name,
      image_id=image_id,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_python_container(packageable: xm.Packageable,
                              py_executable: executables.PythonContainer):
  # Use the directory as the image name.
  image_name = os.path.basename(py_executable.path)
  image_id = build_image.build(py_executable, packageable.args,
                               packageable.env_vars, image_name)
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name, image_id=image_id)


def _package_bazel_container(
    packageable: xm.Packageable,
    container: executables.BazelContainer) -> xm.Executable:
  paths = bazel_client.build_single_target(container.label)
  assert len(paths) == 1
  image_id = docker_adapter.instance().load_image(paths[0])
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name,
      image_id=image_id,
      args=packageable.args,
      env_vars=packageable.env_vars)


def _package_bazel_binary(packageable: xm.Packageable,
                          binary: executables.BazelBinary) -> xm.Executable:
  paths = bazel_client.build_single_target(
      label=binary.label,
      tail_args=binary.bazel_tail_args,
  )
  assert len(paths) == 1
  return local_executables.LocalBinary(
      name=packageable.executable_spec.name,
      path=paths[0],
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _throw_on_unknown_executable(packageable: xm.Packageable, executable: Any):
  raise TypeError('Unsupported executable specification '
                  f'for local packaging: {executable!r}')


_LOCAL_PACKAGING_ROUTER = pattern_matching.match(
    _package_bazel_binary,
    _package_bazel_container,
    _package_binary,
    _package_container,
    _package_dockerfile,
    _package_python_container,
    _throw_on_unknown_executable,
)


def package_for_local_executor(packageable: xm.Packageable,
                               executable_spec: xm.ExecutableSpec):
  return _LOCAL_PACKAGING_ROUTER(packageable, executable_spec)
