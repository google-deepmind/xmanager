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
from typing import Sequence

from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.cloud import build_image
from xmanager.cloud import docker_lib
from xmanager.docker import docker_adapter
from xmanager.xm import executables
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local.packaging import bazel_tools


def _package_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    container: executables.Container,
) -> xm.Executable:
  """Packages a container for local execution."""
  del bazel_outputs
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
        f'{container.image_path} is found neither locally nor remotely'
    )


def _package_binary(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    binary: executables.Binary,
):
  del bazel_outputs
  if not os.path.exists(binary.path):
    raise ValueError(f'{binary.path} does not exist on this machine')
  return local_executables.LocalBinary(
      name=packageable.executable_spec.name,
      command=binary.path,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_dockerfile(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    dockerfile: executables.Dockerfile,
):
  del bazel_outputs
  image_id = docker_lib.build_docker_image(
      dockerfile.name, dockerfile.path, dockerfile.dockerfile
  )
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name,
      image_id=image_id,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_python_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    py_executable: executables.PythonContainer,
):
  del bazel_outputs
  # Use the directory as the image name.
  image_name = os.path.basename(py_executable.path)
  image_id = build_image.build(
      py_executable, packageable.args, packageable.env_vars, image_name
  )
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name, image_id=image_id
  )


def _package_bazel_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    container: executables.BazelContainer,
) -> xm.Executable:
  """Matcher to package BazelContainer."""
  paths = bazel_outputs[
      bazel_client.BazelTarget(
          label=container.label,
          bazel_args=bazel_tools.apply_default_bazel_args(container.bazel_args),
      )
  ]
  assert len(paths) == 1
  image_id = docker_adapter.instance().load_image(paths[0])
  return local_executables.LoadedContainerImage(
      name=packageable.executable_spec.name,
      image_id=image_id,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _select_executable(
    label: str, bazel_args: Sequence[str], paths: Sequence[str]
) -> str:
  """Returns the single runnable among a target's default outputs.

  Fast path: a single default output is returned directly with no Bazel call,
  which covers the common `*_binary` rules that emit exactly one file.

  A target's default output group is not guaranteed to be a single file, though.
  For example, `rules_python`'s `py_binary` reports the input `.py` sources
  (and, under precompilation, `.pyc` artifacts) alongside the launcher. When
  there is more than one default output we can't tell which is the runnable
  from the built paths alone, so we ask Bazel for the target's canonical
  executable
  (`files_to_run.executable`, via `bazel_tools.query_executable_output`) and
  select the built path whose basename matches. This is rule-kind agnostic (not
  `rules_python`-specific): it works for any executable rule, and the extra
  `cquery` is only paid in this ambiguous case.

  Args:
    label: The target label being packaged.
    bazel_args: The build args used for the target, forwarded to the executable
      query so it resolves the identical configuration.
    paths: The target's built default-output paths.

  Returns:
    The path to the target's executable.

  Raises:
    ValueError: If the target is not executable, or its executable cannot be
      unambiguously identified among the built outputs.
  """
  if len(paths) == 1:
    return paths[0]
  executable = bazel_tools.query_executable_output(label, bazel_args)
  if executable is None:
    raise ValueError(
        f'BazelBinary target {label} is not an executable target '
        '(it has no files_to_run.executable).'
    )
  executable_name = os.path.basename(executable)
  matches = [
      path for path in paths if os.path.basename(path) == executable_name
  ]
  if len(matches) != 1:
    raise ValueError(
        'Could not identify the executable among the default outputs of '
        f'{label}: expected exactly one output named {executable_name!r}, got '
        f'{matches!r} (from default outputs {list(paths)!r}).'
    )
  return matches[0]


def _package_bazel_binary(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    binary: executables.BazelBinary,
) -> xm.Executable:
  """Matcher to package BazelBinary."""
  bazel_args = bazel_tools.apply_default_bazel_args(binary.bazel_args)
  paths = bazel_outputs[
      bazel_client.BazelTarget(label=binary.label, bazel_args=bazel_args)
  ]
  return local_executables.LocalBinary(
      name=packageable.executable_spec.name,
      command=_select_executable(binary.label, bazel_args, paths),
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def package_for_local_executor(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    executable_spec: xm.ExecutableSpec,
):
  match executable_spec:
    case executables.BazelBinary() as bazel_binary:
      return _package_bazel_binary(bazel_outputs, packageable, bazel_binary)
    case executables.BazelContainer() as bazel_container:
      return _package_bazel_container(
          bazel_outputs, packageable, bazel_container
      )
    case executables.Binary() as binary:
      return _package_binary(bazel_outputs, packageable, binary)
    case executables.Container() as container:
      return _package_container(bazel_outputs, packageable, container)
    case executables.Dockerfile() as dockerfile:
      return _package_dockerfile(bazel_outputs, packageable, dockerfile)
    case executables.PythonContainer() as python_container:
      return _package_python_container(
          bazel_outputs, packageable, python_container
      )
    case _:
      raise TypeError(
          'Unsupported executable specification '
          f'for local packaging: {executable_spec!r}'
      )
