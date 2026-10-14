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
import datetime
import json
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


def create_tag() -> str:
  return datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')


def prepare_directory(
    destination_directory: str,
    source_directory: str,
    project_name: str,
    entrypoint_file: str,
    dockerfile: str,
) -> None:
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
            (
                'You are trying to pack over 200MB into a Docker image. '
                'Large images negatively impact build times'
            ),
            color='magenta',
        )
    )
  staged_project_dir = os.path.join(destination_directory, project_name)
  shutil.copytree(source_directory, staged_project_dir)
  os.chmod(staged_project_dir, 0o775)
  for root, dirs, files in os.walk(staged_project_dir):
    for d in dirs:
      dir_path = os.path.join(root, d)
      if not os.path.islink(dir_path):
        os.chmod(dir_path, 0o775)
    for f in files:
      file_path = os.path.join(root, f)
      if not os.path.islink(file_path):
        os.chmod(file_path, 0o775)
  shutil.copyfile(dockerfile, os.path.join(destination_directory, 'Dockerfile'))
  staged_entrypoint = os.path.join(destination_directory, 'entrypoint.sh')
  shutil.copyfile(entrypoint_file, staged_entrypoint)
  os.chmod(staged_entrypoint, 0o775)


def is_docker_installed() -> bool:
  """Checks if Docker is installed and accessible."""
  try:
    docker_client = docker.from_env()
    logging.info('Local docker: %s', docker_client.version())
    return True
  except docker.errors.DockerException as e:
    if 'No such file or directory' in str(e):
      # This is the expected case when Docker is not installed, so we don't log
      # anything, and just return False. The other error branches indicate
      # something wrong with the Docker installation, so we log an error and
      # also return False.
      return False
    logging.info(e)
    if 'Permission denied' in str(e):
      print(
          'Looks like there is a permission problem with docker. '
          'Did you install sudoless docker?'
      )
  return False


def build_docker_image(
    image: str,
    directory: str,
    dockerfile: Optional[str] = None,
    use_docker_command: bool = True,
    show_docker_command_progress: bool = False,
) -> str:
  """Builds a Docker image locally."""
  logging.info('Building Docker image')
  docker_client = docker.from_env()
  if not dockerfile:
    dockerfile = os.path.join(directory, 'Dockerfile')
  if use_docker_command:
    _build_image_with_docker_command(
        docker_client,
        directory,
        image,
        dockerfile,
        show_docker_command_progress,
    )
  else:
    _build_image_with_python_client(docker_client, directory, image, dockerfile)
  logging.info('Building docker image: Done')
  return image


def _check_push_for_errors(response: str) -> Optional[str]:
  """Checks a Docker push JSONL response for errors and extracts the digest.

  Args:
    response: Raw string from docker_client.images.push(), containing
      newline-delimited JSON objects.

  Returns:
    The manifest SHA256 digest (`sha256:...`) if present in the stream, or None.

  Raises:
    RuntimeError: If the push response contains an explicit error.
  """
  digest = None
  for line in response.strip().splitlines():
    line = line.strip()
    if not line:
      continue
    try:
      event = json.loads(line)
    except json.JSONDecodeError:
      continue
    if 'error' in event:
      detail = event.get('errorDetail', {}).get('message', event['error'])
      raise RuntimeError(f'Docker push failed: {detail}')
    if aux_digest := event.get('aux', {}).get('Digest'):
      digest = aux_digest
  return digest


def _get_existing_repo_digest(
    docker_client: docker.DockerClient, image: str, repository: str
) -> Optional[str]:
  """Returns an existing remote `repository@sha256:...` digest if pushed."""
  try:
    attrs = docker_client.images.get(image).attrs or {}
    repo_digests = attrs.get('RepoDigests') or []
  except docker.errors.DockerException:
    return None

  prefix = f'{repository}@sha256:'
  for candidate in repo_digests:
    if candidate.startswith(prefix):
      try:
        docker_client.api.inspect_distribution(candidate)
        return candidate
      except docker.errors.DockerException:
        continue
  return None


def push_docker_image(image: str) -> str:
  """Pushes a Docker image to the designated repository."""
  docker_client = docker.from_env()
  repository, _ = docker_utils.parse_repository_tag(image)
  existing_digest = _get_existing_repo_digest(docker_client, image, repository)
  if existing_digest is not None:
    print(
        'Your image URI is:', termcolor.colored(existing_digest, color='blue')
    )
    return existing_digest

  push_response = docker_client.images.push(repository=repository, tag='latest')
  logging.info('%s', push_response)
  # Check for errors embedded in the push response body.
  if not isinstance(push_response, str):
    raise RuntimeError(
        f'Unexpected response type from docker push: {type(push_response)}. '
        'Expected a string.'
    )
  digest = _check_push_for_errors(push_response)
  if not digest:
    # Verify the image is actually accessible in the registry.
    try:
      dist_info = docker_client.api.inspect_distribution(f'{repository}:latest')
    except docker.errors.APIError as e:
      raise RuntimeError(
          f'Push appeared to succeed but image {image} is not accessible in '
          f'the registry. Push response:\n{push_response}'
      ) from e
    digest = dist_info.get('Descriptor', {}).get('digest')

  resolved_image = f'{repository}@{digest}' if digest else image
  print('Your image URI is:', termcolor.colored(resolved_image, color='blue'))
  return resolved_image


def _build_image_with_docker_command(
    client: docker.DockerClient,
    path: str,
    image_tag: str,
    dockerfile: str,
    progress: bool = False,
) -> None:
  """Builds a Docker image by calling `docker build` within a subprocess."""
  version = client.version()['Version']
  [major, minor] = version.split('.')[:2]
  if float(f'{major}.{minor}') < 20.10:
    # docker buildx requires docker 20.10.
    raise RuntimeError('XCloud requires Docker Engine version 20.10+.')
  repository, tag = docker_utils.parse_repository_tag(image_tag)
  if not tag:
    tag = 'latest'
  command = [
      'docker',
      'buildx',
      'build',
      '-t',
      f'{repository}:{tag}',
      '-t',
      f'{repository}:latest',
      '-f',
      dockerfile,
      path,
  ]

  # This is turn makes displaying progress redundant.
  if progress:
    command[3:3] = ['--progress', 'plain']

  subprocess.run(
      command, check=True, env={**os.environ, 'DOCKER_BUILDKIT': '1'}
  )


def _build_image_with_python_client(
    client: docker.DockerClient, path: str, image_tag: str, dockerfile: str
) -> None:
  """Builds a Docker image by calling the Docker Python client."""
  repository, tag = docker_utils.parse_repository_tag(image_tag)
  if not tag:
    tag = 'latest'
  try:
    # The `tag=` arg refers to the full repository:tag image name.
    _, logs = client.images.build(
        path=path, tag=f'{repository}:{tag}', dockerfile=dockerfile
    )
    client.images.build(
        path=path, tag=f'{repository}:latest', dockerfile=dockerfile
    )
  except docker.errors.BuildError as error:
    for log in error.build_log:
      print(log.get('stream', ''), end='', file=sys.stderr)
    raise error
  for log in logs:
    print(log.get('stream', ''), end='')
