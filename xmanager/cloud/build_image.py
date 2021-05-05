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
"""Builds images for XManager Docker executables."""

import os
import shutil
import tempfile
from typing import Dict, Iterable, Optional, Tuple

from absl import flags
from absl import logging
import docker
import requests

from xmanager import xm
from xmanager.cloud import auth
from xmanager.cloud import docker_lib
from xmanager.xm import utils

FLAGS = flags.FLAGS
flags.DEFINE_boolean(
    'use_docker_build_subprocess', True,
    'Call "docker build" in a subprocess rather than using Python docker '
    'client library when building the docker image locally. This provies a '
    'much nicer output for interactive use.')
flags.DEFINE_boolean(
    'wrap_late_bindings', True,
    'Feature flag to wrap and unwrap late bindings for network addresses. '
    'ONLY works with PythonContainer with default instructions or simple '
    'instructions that do not modify the file directory.'
    'REQUIRES ./entrypoint.sh to be the ENTRYPOINT.')

# TODO: Find a master image than is compatible with every
# combination (TF, Torch, JAX) X (CPU, GPU, TPU).
_DEFAULT_BASE_IMAGE = 'gcr.io/deeplearning-platform-release/base-cu110'
_DOCKERFILE_TEMPLATE = """
FROM {base_image}

{instructions}

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x ./entrypoint.sh
{entrypoint}
"""
_ENTRYPOINT_TEMPLATE = """#!/bin/bash

{cmds}
"""


def build(py_executable: xm.PythonContainer,
          args: xm.ArgsType,
          env_vars: Dict[str, str],
          image_name: Optional[str] = None) -> str:
  """Build a Docker image from a Python project."""
  if not image_name:
    image_name = _get_image_name(py_executable)
  dockerfile = _create_dockerfile(py_executable, args, env_vars)
  entrypoint = _create_entrypoint(py_executable)
  arcname = os.path.basename(py_executable.path)
  path = py_executable.path
  if FLAGS.wrap_late_bindings:
    path, dockerfile = _wrap_late_bindings(path, dockerfile)
  docker_directory = docker_lib.prepare_directory(path, arcname, entrypoint,
                                                  dockerfile)
  print('Building Docker image, please wait...')
  try:
    docker_client = docker.from_env()
    logging.info('Local docker: %s', docker_client.version())
  except docker.errors.DockerException as e:
    logging.info(e)
    print('Failed to initialize local docker.')
    print('Falling back to CloudBuild. See INFO log for details.')
  except requests.exceptions.ConnectionError as e:
    logging.info(e)
    if 'Permission denied' in str(e):
      print('Looks like there is a permission problem with docker. '
            'Did you install sudoless docker?')
    else:
      print('Failed to connect to local docker instance.')
    print('Falling back to CloudBuild. See INFO log for details.')
  else:
    # TODO: Improve out-of-disk space handling.
    return docker_lib.build_docker_image(image_name, docker_directory)
  # TODO: Also add the CloudBuild case.
  # This method assumes that the image will be available locally.
  # So, the implementation should also do a pull after building.
  raise Exception('CloudBuild not implemented.')


def push(image: str) -> str:
  return docker_lib.push_docker_image(image)


def _get_image_name(py_executable: xm.PythonContainer) -> str:
  image_name = os.path.basename(py_executable.path)
  project_name = auth.get_project_name()
  return f'gcr.io/{project_name}/{image_name}:latest'


def _get_base_image(py_executable: xm.PythonContainer) -> str:
  if py_executable.base_image:
    return py_executable.base_image
  return _DEFAULT_BASE_IMAGE


def _create_instructions(py_executable: xm.PythonContainer,
                         env_vars: Dict[str, str]) -> str:
  """Create Docker instructions."""
  set_env_vars = [f'ENV {key}="{value}"' for key, value in env_vars.items()]
  if py_executable.docker_instructions:
    return '\n'.join(py_executable.docker_instructions + set_env_vars)

  directory = os.path.basename(py_executable.path)
  return '\n'.join(list(_default_steps(directory)) + set_env_vars)


def _default_steps(directory: str) -> Iterable[str]:
  return (
      # Without setting LANG, RDL ran into an UnicodeDecodeError, similar to
      # what is described at [1]. This seems to be good practice and not hurt so
      # we're just always setting it.
      # [1] https://github.com/spotDL/spotify-downloader/issues/279
      'ENV LANG C.UTF-8',
      # Updating and installing on the same line causes cache-busting.
      # https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#run
      'RUN apt-get update && apt-get install -y git',
      'RUN python -m pip install --upgrade pip',
      f'COPY {directory}/requirements.txt {directory}/requirements.txt',
      f'RUN python -m pip install -r {directory}/requirements.txt',
      # It is best practice to copy the project directory as late as possible,
      # rather than at the beginning. This allows Docker to reuse cached layers.
      # If copying the project files were the first step, a tiny modification to
      # the source code will invalidate the cache.
      # https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#add-or-copy
      f'COPY {directory}/ {directory}',
      f'WORKDIR {directory}',
  )


def _create_dockerfile(py_executable: xm.PythonContainer, args: xm.ArgsType,
                       env_vars: Dict[str, str]) -> str:
  """Creates a Dockerfile from a project executable."""
  base_image = _get_base_image(py_executable)
  instructions = _create_instructions(py_executable, env_vars)
  entrypoint = _create_entrypoint_cmd(args)
  contents = _DOCKERFILE_TEMPLATE.format(
      base_image=base_image,
      instructions=instructions,
      entrypoint=entrypoint)
  print('Dockerfile:', contents, sep='\n')
  t = tempfile.NamedTemporaryFile(delete=False)
  with open(t.name, 'w') as f:
    f.write(contents)
  return t.name


def _create_entrypoint(py_executable: xm.PythonContainer) -> str:
  """Create a bash entrypoint based on the base image."""
  if isinstance(py_executable.entrypoint, xm.ModuleName):
    cmds = f'python -m {py_executable.entrypoint.module_name} $@'
  elif isinstance(py_executable.entrypoint, xm.CommandList):
    cmds = '\n'.join(py_executable.entrypoint.commands) + ' $@'
  else:
    raise ValueError('Unsupported entrypoint type {}'.format(
        type(py_executable.entrypoint)))
  contents = _ENTRYPOINT_TEMPLATE.format(cmds=cmds)

  t = tempfile.NamedTemporaryFile(delete=False)
  with open(t.name, 'w') as f:
    f.write(contents)
  return t.name


def _create_entrypoint_cmd(args: xm.ArgsType) -> str:
  """Create the entrypoint command with optional args."""
  entrypoint_args = ['./entrypoint.sh']
  entrypoint_args += utils.to_command_line_args(args)
  entrypoint = ', '.join([f'"{arg}"' for arg in entrypoint_args])
  return f'ENTRYPOINT [{entrypoint}]'


def _wrap_late_bindings(path: str, dockerfile: str) -> Tuple[str, str]:
  """Create a new path and dockerfile to wrap/unwrap late-bindings.

  TODO: Rather than only working PythonContainer, this method can
  also work on PrebuiltContainers. We do this by inspecting the entrypoint by
  using `docker.APIClient().inspect_image()`.

  Late bindings are special formatted strings that are evaluated at runtime. The
  primary use for late-bindings is to find the address of other jobs in CAIP
  which is only known at runtime and cannot be statically defined.

  Args:
    path: The current project path to build.
    dockerfile: The current dockerfile path to use to build.

  Returns:
    A project path to build and a new dockerfile path to build with.
  """
  new_path = tempfile.TemporaryDirectory()
  # In Python 3.6 shutil.copytree, the destination must not exist.
  new_path.cleanup()
  shutil.copytree(path, new_path.name)

  dirname = os.path.dirname(os.path.realpath(__file__))
  shutil.copyfile(
      os.path.join(dirname, 'data', 'wrapped_entrypoint.sh'),
      os.path.join(new_path.name, 'wrapped_entrypoint.sh'))
  shutil.copyfile(
      os.path.join(dirname, 'utils.py'),
      os.path.join(new_path.name, 'caip_utils.py'))

  new_dockerfile = tempfile.NamedTemporaryFile(delete=False)
  insert_instructions = [
      'RUN chmod +x ./wrapped_entrypoint.sh',
  ]
  with open(dockerfile) as f:
    contents = f.read()
    contents = contents.replace('ENTRYPOINT',
                                '\n'.join(insert_instructions + ['ENTRYPOINT']))
    contents = contents.replace('ENTRYPOINT ["./entrypoint.sh',
                                'ENTRYPOINT ["./wrapped_entrypoint.sh')
    with open(new_dockerfile.name, 'w') as f:
      f.write(contents)
  print()

  # TODO: Remove the disable once the pytype bug is fixed.
  return new_path.name, new_dockerfile.name  # pytype: disable=bad-return-type
