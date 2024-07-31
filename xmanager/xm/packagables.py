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

from typing import Any, Collection, List, Mapping, Optional, Union

import immutabledict
from xmanager.xm import executables
from xmanager.xm import job_blocks


# NOTE: Do not edit methods below manually.
# They should be generated with packagables_generator utility.


def binary(
    executor_spec: job_blocks.ExecutorSpec,
    path: str,
    dependencies: Collection[executables.BinaryDependency] = (),
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """A prebuilt executable program.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    path: Path to a prebuilt binary.
    dependencies: A list of data dependencies to be packaged together with the
      binary.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.Binary(
          path=path,
          dependencies=dependencies,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )


def bazel_binary(
    executor_spec: job_blocks.ExecutorSpec,
    label: str,
    dependencies: Collection[executables.BinaryDependency] = (),
    bazel_args: Collection[str] = (),
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """A Bazel target that produces a self-contained binary.

  Note that for Python targets based on https://github.com/google/subpar
  a self-contained '.par' binary would be built.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    label: The Bazel target to be built.
    dependencies: A list of data dependencies to be packaged together with
      the binary.
    bazel_args: Bazel command line arguments.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.BazelBinary(
          label=label,
          dependencies=dependencies,
          bazel_args=bazel_args,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )


def container(
    executor_spec: job_blocks.ExecutorSpec,
    image_path: str,
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """A prebuilt Docker image.

  The image can be tagged locally or in a remote repository.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    image_path: Path to a prebuilt container image.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.Container(
          image_path=image_path,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )


def bazel_container(
    executor_spec: job_blocks.ExecutorSpec,
    label: str,
    bazel_args: Collection[str] = (),
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """A Bazel target that produces a .tar image.

  Note that for targets based on https://github.com/bazelbuild/rules_docker one
  should append '.tar' to the label to specify a self-contained image.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    label: The Bazel target to be built.
    bazel_args: Bazel command line arguments.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.BazelContainer(
          label=label,
          bazel_args=bazel_args,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )


def python_container(
    executor_spec: job_blocks.ExecutorSpec,
    entrypoint: Union[executables.ModuleName, executables.CommandList],
    path: str = '.',
    base_image: Optional[str] = None,
    docker_instructions: Optional[List[str]] = None,
    use_deep_module: bool = False,
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """PythonContainer describes a directory containing Python code.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    entrypoint: The Python module or list of shell commands to run when entering
      this Python project.
    path: Relative or absolute path to the Python project. By default, the
      current directory (`'.'`) is used.
    base_image: Name of the image to initialize a new Docker build stage using
      the instruction `FROM`.
    docker_instructions: List of Docker instructions to apply when building the
      image. If not specified, the default one will be provided.

      When you use `docker_instructions`, you are responsible for copying the
      project directory. For example, if you are running with:

        path='/path/to/cifar10'

      You should include these steps in your `docker_instructions`:

        [
          'COPY cifar10/ cifar10',
          'WORKDIR cifar10',
        ]

      If your source code rarely changes, you can make this your first step.
      If you are frequently iterating on the source code, it is best practice
      to place these steps as late as possible in the list to maximize Docker
      layer-caching.
    use_deep_module: Whether the experiment code uses deep module structure
      (i.e., 'from <a.prefix> import models') or not (i.e., 'import models').

      If use_deep_module is set to True, and docker_instructions are used, it
      is recommended to use dedicated workdir and copy a whole project
      directory there. The example above should be modified as:

        [
          'RUN mkdir /workdir',
          'WORKDIR /workdir',
          'COPY cifar10/ /workdir/cifar10',
        ]
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.PythonContainer(
          entrypoint=entrypoint,
          path=path,
          base_image=base_image,
          docker_instructions=docker_instructions,
          use_deep_module=use_deep_module,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )


def dockerfile_container(
    executor_spec: job_blocks.ExecutorSpec,
    path: str = '.',
    dockerfile: Optional[str] = None,
    *,
    args: Optional[job_blocks.UserArgs] = None,
    env_vars: Mapping[str, Any] = immutabledict.immutabledict(),
) -> job_blocks.Packageable:
  # pyformat: disable
  """Dockerfile describes a Dockerfile for generating a docker image.

  This is a lower-level feature that could be solved using higher-level
  Executables such as BazelContainer or PythonContainer.

  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged.
    path: Specifies the build's context.
    dockerfile: The file that will be used for build instructions. Otherwise,
      {path}/Dockerfile will be used. Equivalent to `docker build -f`. A
      relative path will use a Dockerfile that is relative to the launcher
      script.
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as --batch_size=16. If positional arguments
      are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.Dockerfile(
          path=path,
          dockerfile=dockerfile,
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )
