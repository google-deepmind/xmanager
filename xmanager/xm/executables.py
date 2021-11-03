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
"""Definition of shared executable specifications."""

import abc
import os
import re
from typing import List, NamedTuple, Optional, Union

import attr
from xmanager.xm import job_blocks
from xmanager.xm import utils


def name_from_path(path: str) -> str:
  """Returns a safe to use executable name based on a filesystem path."""
  return re.sub('\\W', '_', os.path.basename(path.rstrip(os.sep)))


class ModuleName(NamedTuple):
  """Name of python module to execute when entering this project."""
  module_name: str


class CommandList(NamedTuple):
  """List of commands to execute when entering this project."""
  commands: List[str]


@attr.s(auto_attribs=True)
class Dockerfile(job_blocks.ExecutableSpec):
  """Dockerfile describes a Dockerfile for generating a docker image.

  This is a lower-level feature that could be solved using higher-level
  Executables such as BazelContainer or PythonContainer.

  Attributes:
    path: Specifies the build's context.
    dockerfile: The file that will be used for build instructions. Otherwise,
      {path}/Dockerfile will be used. Equivalent to `docker build -f`. A
      relative path will use a Dockerfile that is relative to the launcher
      script.
  """

  path: str = attr.ib(
      converter=utils.resolve_path_relative_to_launcher, default='.')
  dockerfile: str = attr.ib(
      # This field is always set once the object is initialized, so we use str
      # as type annotation. But the default value depends on another property
      # and is set in __attrs_post_init__, so we temporary convert None to ''.
      converter=lambda dockerfile: dockerfile or '',
      default=None)

  def __attrs_post_init__(self):
    if not self.dockerfile:
      self.dockerfile = os.path.join(self.path, 'Dockerfile')
    self.dockerfile = utils.resolve_path_relative_to_launcher(self.dockerfile)

  @property
  def name(self) -> str:
    return name_from_path(self.path)


@attr.s(auto_attribs=True)
class PythonContainer(job_blocks.ExecutableSpec):
  """PythonContainer describes a directory containing Python code.

  Attributes:
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
  """

  entrypoint: Union[ModuleName, CommandList]
  path: str = attr.ib(
      converter=utils.resolve_path_relative_to_launcher, default='.')
  base_image: Optional[str] = None
  docker_instructions: Optional[List[str]] = None
  use_deep_module: bool = False

  @property
  def name(self) -> str:
    return name_from_path(self.path)


class BinaryDependency(abc.ABC):
  """Additional resource for `Binary` / `BazelBinary`.

  Implementations can define backend-specific dependencies.
  """


@attr.s(auto_attribs=True)
class Container(job_blocks.ExecutableSpec):
  """A prebuilt Docker image.

  The image can be tagged locally or in a remote repository.

  Attributes:
    image_path: Path to a prebuilt container image.
  """

  image_path: str

  @property
  def name(self) -> str:
    return name_from_path(self.image_path)


@attr.s(auto_attribs=True)
class Binary(job_blocks.ExecutableSpec):
  """A prebuilt executable program.

  Attributes:
    path: Path to a prebuilt binary.
    dependencies: A list of data dependencies to be packaged together with the
      binary.
  """

  path: str
  dependencies: List[BinaryDependency] = attr.ib(
      converter=list, default=attr.Factory(list))

  @property
  def name(self) -> str:
    return name_from_path(self.path)


@attr.s(auto_attribs=True)
class BazelContainer(job_blocks.ExecutableSpec):
  """A Bazel target that produces a .tar image.

  Note that for targets based on https://github.com/bazelbuild/rules_docker one
  should append '.tar' to the label to specify a self-contained image.

  Attributes:
    label: The Bazel target to be built.
    bazel_args: Bazel command line arguments.
  """

  label: str
  bazel_args: List[str] = attr.ib(converter=list, default=attr.Factory(list))

  @property
  def name(self) -> str:
    return name_from_path(self.label)


@attr.s(auto_attribs=True)
class BazelBinary(job_blocks.ExecutableSpec):
  """A Bazel target that produces a self-contained binary.

  Note that for Python targets based on https://github.com/google/subpar
  a self-contained '.par' binary would be built.

  Attributes:
    label: The Bazel target to be built.
    dependencies: A list of data dependencies to be packaged together with
      the binary.
    bazel_args: Bazel command line arguments.
  """

  label: str
  dependencies: List[BinaryDependency] = attr.ib(
      converter=list, default=attr.Factory(list))
  bazel_args: List[str] = attr.ib(converter=list, default=attr.Factory(list))

  @property
  def name(self) -> str:
    return name_from_path(self.label)
