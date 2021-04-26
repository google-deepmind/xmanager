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
import subprocess
import tarfile
import tempfile

from absl import logging
import docker
import termcolor


def build_tar(project_path: str, arcname: str, entrypoint_file: str,
              dockerfile: str) -> str:
  """Creates a tar.gz with all the project contents and Dockerfile."""
  folder = tempfile.mkdtemp()
  tar_name = os.path.join(folder, 'tmp.tar.gz')
  with tarfile.open(tar_name, 'w:gz') as tar:
    tar.add(project_path, arcname=arcname)
    tar.add(dockerfile, arcname='Dockerfile')
    tar.add(entrypoint_file, arcname='entrypoint.sh')
  return tar_name


def build_docker_image(image: str,
                       tar: str,
                       docker_subprocess: bool = True) -> str:
  """Builds a Docker image locally."""
  logging.info('Building Docker image locally')
  extracted = tempfile.mkdtemp()
  with tarfile.open(tar) as t:
    t.extractall(path=extracted)
  docker_client = docker.from_env()
  if docker_subprocess:
    _run_docker_in_subprocess(docker_client, extracted, image)
  else:
    _run_docker_build(docker_client, extracted, image)
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
        print('Pulling image', raw_image_name)
        client.images.pull(repository=raw_image_name)
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
