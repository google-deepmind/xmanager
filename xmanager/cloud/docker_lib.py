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
import tempfile

from absl import logging
import docker
from docker.utils import utils as docker_utils
import humanize
import termcolor


def prepare_directory(project_path: str, project_name: str,
                      entrypoint_file: str, dockerfile: str) -> str:
  """Stage all inputs into a new temporary directory."""
  project_directory = pathlib.Path(project_path)
  size = sum(
      f.stat().st_size for f in project_directory.glob('**/*') if f.is_file())
  print(f'Size of Docker input: {humanize.naturalsize(size)}')
  if size > 200 * 10**6:
    print(
        termcolor.colored(
            'Your trying to pack over 200MB into the Docker image. '
            'Large images negatively impact build times.',
            color='magenta'))
  directory = tempfile.mkdtemp()
  shutil.copytree(project_path, os.path.join(directory, project_name))
  shutil.copyfile(dockerfile, os.path.join(directory, 'Dockerfile'))
  shutil.copyfile(entrypoint_file, os.path.join(directory, 'entrypoint.sh'))
  return directory


def build_docker_image(image: str,
                       directory: str,
                       docker_subprocess: bool = True) -> str:
  """Builds a Docker image locally."""
  logging.info('Building Docker image locally')
  docker_client = docker.from_env()
  if docker_subprocess:
    _run_docker_in_subprocess(docker_client, directory, image)
  else:
    _run_docker_build(docker_client, directory, image)
  logging.info('Building docker image locally: Done')
  return image


def push_docker_image(image: str):
  """Pushes a Docker image to the designated repository."""
  docker_client = docker.from_env()
  push = docker_client.images.push(repository=image)
  logging.info(push)
  if not isinstance(push, str) or '"Digest":' not in push:
    raise Exception(
        'Expected docker push to return a string with `status: Pushed` and a '
        'Digest. This is probably a temporary issue with --build_locally and '
        'you should try again')
  print('Your image URI is:', termcolor.colored(image, color='blue'))
  return image


def _run_docker_in_subprocess(client: docker.DockerClient, path: str,
                              image: str) -> None:
  """Builds a Docker image by calling `docker build` within a subprocess."""
  # "Pre-pulling" the image in Dockerfile so that the docker build subprocess
  # (next command) can pull from cache (see b/174748727 for more details).
  with open(os.path.join(path, 'Dockerfile'), 'r') as f:
    for line in f:
      if 'FROM' in line:
        line = line.strip()
        raw_image_name = line.split(' ', 1)[1]
        repository, tag = docker_utils.parse_repository_tag(raw_image_name)
        if tag is None:
          tag = 'latest'
        print(f'Pulling image {repository}:{tag}')
        # Without a tag, docker will try to pull every image instead of latest.
        # From docker>=4.4.0, use client.image.pull(*args, all_tags=False)
        client.images.pull(repository=repository, tag=tag)
        break

  subprocess.run(['docker', 'build', '-t', image, path],
                 check=True,
                 env={'DOCKER_BUILDKIT': '1'})


def _run_docker_build(client: docker.DockerClient, path: str,
                      image: str) -> None:
  """Builds a Docker image by calling the Docker Python client."""
  try:
    _, logs = client.images.build(path=path, tag=image)
  except docker.errors.BuildError as e:
    for l in e.build_log:
      print(l.get('stream', ''), end='')
    raise e
  for l in logs:
    print(l.get('stream', ''), end='')
