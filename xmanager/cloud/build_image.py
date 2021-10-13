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
from typing import Dict, List, Optional

from absl import flags
from absl import logging
import docker
from docker.utils import utils as docker_utils
import requests

from xmanager import xm
from xmanager.cloud import auth
from xmanager.cloud import cloud_build
from xmanager.cloud import docker_lib
from xmanager.docker import docker_adapter
from xmanager.xm import utils

_BUILD_IMAGE_LOCALLY = flags.DEFINE_boolean(
    'xm_build_image_locally', True,
    'Use local Docker to build images instead of remote Google Cloud Build. '
    'This is usually a lot faster but requires docker to be installed.')
_USE_DOCKER_COMMAND = flags.DEFINE_boolean(
    'xm_use_docker_command', True,
    'Call "docker build" in a subprocess rather than using Python docker '
    'client library when building the docker image locally. This provies a '
    'much nicer output for interactive use.')
_SHOW_DOCKER_COMMAND_PROGRESS = flags.DEFINE_boolean(
    'xm_show_docker_command_progress', False,
    'Show container output during the "docker build".')
_WRAP_LATE_BINDINGS = flags.DEFINE_boolean(
    'xm_wrap_late_bindings', False,
    'Feature flag to wrap and unwrap late bindings for network addresses. '
    'ONLY works with PythonContainer with default instructions or simple '
    'instructions that do not modify the file directory. '
    'REQUIRES ./entrypoint.sh to be the ENTRYPOINT.')

# TODO: Find a master image than is compatible with every
# combination (TF, Torch, JAX) X (CPU, GPU, TPU).
_DEFAULT_BASE_IMAGE = 'gcr.io/deeplearning-platform-release/base-cu110'
_DOCKERFILE_TEMPLATE = """
FROM {base_image}

RUN if ! id 1000; then useradd -m -u 1000 clouduser; fi

{instructions}

COPY entrypoint.sh ./entrypoint.sh
RUN chown -R 1000:root ./entrypoint.sh && chmod -R 775 ./entrypoint.sh

{entrypoint}
"""
_ENTRYPOINT_TEMPLATE = """#!/bin/bash

{cmds}
"""


def build(py_executable: xm.PythonContainer,
          args: xm.SequentialArgs,
          env_vars: Dict[str, str],
          image_name: Optional[str] = None,
          project: Optional[str] = None,
          bucket: Optional[str] = None) -> str:
  """Build a Docker image from a Python project.

  Args:
    py_executable: The PythonContainer to build.
    args: Args to pass to the image.
    env_vars: Environment variables to set in the image.
    image_name: The image name that will be assigned to the resulting image.
    project: The project to use if CloudBuild is used.
    bucket: The bucket to upload if CloudBuild is used.

  Returns:
    The name of the built image.
  """
  if not image_name:
    image_name = _get_image_name(py_executable)
  dockerfile = _create_dockerfile(py_executable, args, env_vars)
  entrypoint = _create_entrypoint(py_executable)
  dirname = os.path.basename(py_executable.path)
  python_path = py_executable.path

  with tempfile.TemporaryDirectory() as wrapped_directory:
    if _WRAP_LATE_BINDINGS.value:
      _wrap_late_bindings(wrapped_directory, python_path, dockerfile)
      python_path = wrapped_directory
      dockerfile = os.path.join(python_path, 'Dockerfile')

    with tempfile.TemporaryDirectory() as staging:
      docker_lib.prepare_directory(staging, python_path, dirname, entrypoint,
                                   dockerfile)
      return build_by_dockerfile(staging, os.path.join(staging, 'Dockerfile'),
                                 image_name, project, bucket)


def build_by_dockerfile(path: str,
                        dockerfile: str,
                        image_name: str,
                        project: Optional[str] = None,
                        bucket: Optional[str] = None):
  """Build a Docker image from a Docker directory.

  Args:
    path: The directory to use for the Docker build context.
    dockerfile: The path of Dockerfile.
    image_name: The name to set the built image to.
    project: The project to use if CloudBuild is used.
    bucket: The bucket to upload if CloudBuild is used.

  Returns:
    The name of the built image.
  """
  print('Building Docker image, please wait...')
  if _BUILD_IMAGE_LOCALLY.value:
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
      return docker_lib.build_docker_image(
          image_name,
          path,
          dockerfile,
          use_docker_command=_USE_DOCKER_COMMAND.value,
          show_docker_command_progress=_SHOW_DOCKER_COMMAND_PROGRESS.value)

  # If Dockerfile is not a direct child of path, then create a temp directory
  # that contains both the contents of path and Dockerfile.
  with tempfile.TemporaryDirectory() as tempdir:
    if os.path.dirname(dockerfile) != path:
      new_path = os.path.join(tempdir, os.path.basename(path))
      shutil.copytree(path, new_path)
      shutil.copyfile(dockerfile, os.path.join(path, 'Dockerfile'))
      path = new_path

    cloud_build_client = cloud_build.Client(project=project, bucket=bucket)
    repository, _ = docker_utils.parse_repository_tag(image_name)
    upload_name = repository.split('/')[-1]
    cloud_build_client.build_docker_image(image_name, path, upload_name)
    docker_adapter.instance().pull_image(image_name)
    return image_name


def push(image: str) -> str:
  return docker_lib.push_docker_image(image)


def _get_image_name(py_executable: xm.PythonContainer) -> str:
  image_name = os.path.basename(py_executable.path)
  project_name = auth.get_project_name()
  tag = docker_lib.create_tag()
  return f'gcr.io/{project_name}/{image_name}:{tag}'


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
  return '\n'.join(
      list(default_steps(directory, py_executable.use_deep_module)) +
      set_env_vars)


def default_steps(directory: str, use_deep_module: bool) -> List[str]:
  """Default commands to use in the Dockerfile."""
  workdir_setup_prefix = []
  workdir_setup_suffix = []
  project_dir = f'/{directory}'
  if use_deep_module:
    # Setting a top-level work dir allows using the Python code without
    # modifying import statements.
    workdir_setup_prefix = [
        'RUN mkdir /workdir',
        'WORKDIR /workdir',
    ]
    project_dir = f'/workdir/{directory}'
  else:
    workdir_setup_suffix = [
        f'WORKDIR {directory}',
    ]

  return workdir_setup_prefix + [
      # Without setting LANG, RDL ran into an UnicodeDecodeError, similar to
      # what is described at [1]. This seems to be good practice and not hurt so
      # we're just always setting it.
      # [1] https://github.com/spotDL/spotify-downloader/issues/279
      'ENV LANG=C.UTF-8',
      # Updating and installing on the same line causes cache-busting.
      # https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#run
      'RUN apt-get update && apt-get install -y git netcat',
      'RUN python -m pip install --upgrade pip setuptools',
      f'COPY {directory}/requirements.txt {project_dir}/requirements.txt',
      f'RUN python -m pip install -r {directory}/requirements.txt',
      # It is best practice to copy the project directory as late as possible,
      # rather than at the beginning. This allows Docker to reuse cached layers.
      # If copying the project files were the first step, a tiny modification to
      # the source code will invalidate the cache.
      # https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#add-or-copy
      f'COPY {directory}/ {project_dir}',
      # Changing ownwership of project_dir, so that both users: UID 1000
      # and root are the co-owner of it.
      f'RUN chown -R 1000:root {project_dir} && chmod -R 775 {project_dir}',
  ] + workdir_setup_suffix


def _create_dockerfile(
    py_executable: xm.PythonContainer,
    args: xm.SequentialArgs,
    env_vars: Dict[str, str],
) -> str:
  """Creates a Dockerfile from a project executable."""
  base_image = _get_base_image(py_executable)
  instructions = _create_instructions(py_executable, env_vars)
  entrypoint = _create_entrypoint_cmd(args)
  contents = _DOCKERFILE_TEMPLATE.format(
      base_image=base_image, instructions=instructions, entrypoint=entrypoint)
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


def _create_entrypoint_cmd(args: xm.SequentialArgs) -> str:
  """Create the entrypoint command with optional args."""
  entrypoint_args = ['./entrypoint.sh']
  entrypoint_args.extend(args.to_list(utils.ARG_ESCAPER))
  entrypoint = ', '.join([f'"{arg}"' for arg in entrypoint_args])
  return f'ENTRYPOINT [{entrypoint}]'


def _wrap_late_bindings(destination: str, path: str, dockerfile: str) -> None:
  """Create a new path and dockerfile to wrap/unwrap late-bindings.

  TODO: Rather than only working PythonContainer, this method can
  also work on PrebuiltContainers. We do this by inspecting the entrypoint by
  using `docker.APIClient().inspect_image()`.

  Late bindings are special formatted strings that are evaluated at runtime. The
  primary use for late-bindings is to find the address of other jobs in CAIP
  which is only known at runtime and cannot be statically defined.

  Args:
    destination: An empty destination to contain the new project path
      and the new dockerfile will be destination/Dockerfile.
      The current contents of destination will be deleted.
    path: The current project path to build.
    dockerfile: The current dockerfile path needed to build the project.
  """
  shutil.rmtree(destination)
  shutil.copytree(path, destination)

  root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

  shutil.copyfile(
      os.path.join(root_dir, 'cloud', 'data', 'wrapped_entrypoint.sh'),
      os.path.join(destination, 'wrapped_entrypoint.sh'))
  shutil.copyfile(
      os.path.join(root_dir, 'cloud', 'utils.py'),
      os.path.join(destination, 'caip_utils.py'))
  shutil.copyfile(
      os.path.join(root_dir, 'vizier', 'vizier_worker.py'),
      os.path.join(destination, 'vizier_worker.py'))

  new_dockerfile = os.path.join(destination, 'Dockerfile')
  insert_instructions = [
      'RUN chmod +x ./wrapped_entrypoint.sh',
  ]
  with open(dockerfile) as f:
    contents = f.read()
  contents = contents.replace('ENTRYPOINT',
                              '\n'.join(insert_instructions + ['ENTRYPOINT']))
  contents = contents.replace('ENTRYPOINT ["./entrypoint.sh',
                              'ENTRYPOINT ["./wrapped_entrypoint.sh')
  with open(new_dockerfile, 'w') as f:
    f.write(contents)
