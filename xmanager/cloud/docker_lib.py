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
"""Utility functions for building Docker images."""
import os
import pathlib
import shutil
import subprocess
import sys
from typing import Optional

from absl import logging
import docker
from docker.utils import utils as docker_utils
import humanize
import termcolor


def prepare_directory(destination_directory: str, source_directory: str,
                      project_name: str, entrypoint_file: str,
                      dockerfile: str) -> None:
  """Stage all inputs into the destination directory.

  Args:
    destination_directory: The directory to copy files to.
    source_directory: The directory to copy files from.
    project_name: The name of the folder inside destination_directory/ that
      source_directory/ files will be copied to.
    entrypoint_file: The file path of entrypoint.sh.
    dockerfile: The file path of Dockerfile.
  """
  source_path = pathlib.Path(source_directory)
  size = sum(f.stat().st_size for f in source_path.glob('**/*') if f.is_file())
  print(f'Size of Docker input: {humanize.naturalsize(size)}')
  if size > 200 * 10**6:
    print(
        termcolor.colored(
            'You are trying to pack over 200MB into a Docker image. '
            'Large images negatively impact build times',
            color='magenta'))
  shutil.copytree(source_directory,
                  os.path.join(destination_directory, project_name))
  shutil.copyfile(dockerfile, os.path.join(destination_directory, 'Dockerfile'))
  shutil.copyfile(entrypoint_file,
                  os.path.join(destination_directory, 'entrypoint.sh'))


def build_docker_image(image: str,
                       directory: str,
                       dockerfile: Optional[str] = None,
                       use_docker_command: bool = True,
                       show_docker_command_progress: bool = False) -> str:
  """Builds a Docker image locally."""
  logging.info('Building Docker image')
  docker_client = docker.from_env()
  if not dockerfile:
    dockerfile = os.path.join(directory, 'Dockerfile')
  if use_docker_command:
    _build_image_with_docker_command(directory, image, dockerfile,
                                     show_docker_command_progress)
  else:
    _build_image_with_python_client(docker_client, directory, image, dockerfile)
  logging.info('Building docker image: Done')
  return image


def push_docker_image(image: str) -> str:
  """Pushes a Docker image to the designated repository."""
  docker_client = docker.from_env()
  repository, tag = docker_utils.parse_repository_tag(image)
  push = docker_client.images.push(repository=repository, tag=tag)
  logging.info(push)
  if not isinstance(push, str) or '"Digest":' not in push:
    raise RuntimeError(
        'Expected docker push to return a string with `status: Pushed` and a '
        'Digest. This is probably a temporary issue with --build_locally and '
        'you should try again')
  print('Your image URI is:', termcolor.colored(image, color='blue'))
  return image


def _build_image_with_docker_command(path: str,
                                     image_tag: str,
                                     dockerfile: str,
                                     progress: bool = False) -> None:
  """Builds a Docker image by calling `docker build` within a subprocess."""
  # docker buildx requires docker 20.10.
  command = [
      'docker', 'buildx', 'build', '-t', image_tag, '-f', dockerfile, path
  ]

  # Adding flags to show progress and disabling cache.
  # Caching prevents actual commands in layer from executing.
  # This is turn makes displaying progress redundant.
  if progress:
    command[2:2] = ['--progress', 'plain', '--no-cache']

  subprocess.run(command, check=True, env={'DOCKER_BUILDKIT': '1'})


def _build_image_with_python_client(client: docker.DockerClient, path: str,
                                    image_tag: str, dockerfile: str) -> None:
  """Builds a Docker image by calling the Docker Python client."""
  try:
    # The `tag=` arg refers to the full repository:tag image name.
    _, logs = client.images.build(
        path=path, tag=image_tag, dockerfile=dockerfile)
  except docker.errors.BuildError as error:
    for log in error.build_log:
      print(log.get('stream', ''), end='', file=sys.stderr)
    raise error
  for log in logs:
    print(log.get('stream', ''), end='')
