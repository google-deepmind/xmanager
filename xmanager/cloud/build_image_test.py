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
import json
import os
import tempfile
from unittest import mock

from absl.testing import absltest
import docker
from xmanager import xm
from xmanager.cloud import build_image
from xmanager.cloud import docker_lib


class BuildImageTest(absltest.TestCase):

  def create_container(self, entrypoint) -> xm.PythonContainer:
    return xm.PythonContainer(entrypoint=entrypoint)

  def test_get_entrypoint_commands_module_adds_suffix(self):
    project = self.create_container(xm.ModuleName('some.python.module'))
    entrypoint_commands = build_image._get_entrypoint_commands(project)
    self.assertEndsWith(entrypoint_commands, ' "$@"')

  def test_get_entrypoint_commands_adds_suffix(self):
    commands = ['echo "aaa"']
    project = self.create_container(xm.CommandList(commands))
    entrypoint_commands = build_image._get_entrypoint_commands(project)
    self.assertEndsWith(entrypoint_commands, ' "$@"')

  def test_get_entrypoint_commands_no_dup_plain_suffix(self):
    commands = ['echo "aaa" $@']
    project = self.create_container(xm.CommandList(commands))
    entrypoint_commands = build_image._get_entrypoint_commands(project)
    self.assertEndsWith(entrypoint_commands, ' $@')

  def test_get_entrypoint_commands_no_dup_quoted_suffix(self):
    commands = ['echo "aaa" "$@"']
    project = self.create_container(xm.CommandList(commands))
    entrypoint_commands = build_image._get_entrypoint_commands(project)
    self.assertEndsWith(entrypoint_commands, ' "$@"')
    self.assertNotEndsWith(entrypoint_commands, ' "$@" "$@"')

  def test_get_entrypoint_commands_dup_single_quoted_suffix(self):
    commands = ['echo "aaa" \'$@\'']
    project = self.create_container(xm.CommandList(commands))
    entrypoint_commands = build_image._get_entrypoint_commands(project)
    self.assertEndsWith(entrypoint_commands, ' \'$@\' "$@"')

  def test_create_entrypoint_cmd_preserves_argv(self):
    args = xm.SequentialArgs.from_collection({
        'config': '{"key": 4}',
        'path': 'a path',
    })

    instruction = build_image._create_entrypoint_cmd(args)

    self.assertEqual(
        json.loads(instruction.removeprefix('ENTRYPOINT ')),
        [
            './entrypoint.sh',
            '--config={"key": 4}',
            '--path=a path',
        ],
    )

  def test_default_steps_without_deep_module(self):
    steps = build_image.default_steps('my_dir', use_deep_module=False)
    pip_upgrade_idx = steps.index('RUN python -m pip install --upgrade pip')
    mkdir_idx = steps.index(
        'RUN mkdir -p -m 775 /my_dir && chown 1000:root /my_dir'
    )
    copy_req_idx = steps.index(
        'COPY --chown=1000:root my_dir/requirements.txt'
        ' /my_dir/requirements.txt'
    )
    self.assertLess(pip_upgrade_idx, mkdir_idx)
    self.assertLess(mkdir_idx, copy_req_idx)
    self.assertIn('COPY --chown=1000:root my_dir/ /my_dir', steps)
    self.assertIn('WORKDIR my_dir', steps)

  def test_default_steps_with_deep_module(self):
    steps = build_image.default_steps('my_dir', use_deep_module=True)
    self.assertEqual(
        steps[:2],
        [
            'RUN mkdir -p -m 775 /workdir && chown 1000:root /workdir',
            'WORKDIR /workdir',
        ],
    )
    pip_upgrade_idx = steps.index('RUN python -m pip install --upgrade pip')
    mkdir_idx = steps.index(
        'RUN mkdir -p -m 775 /workdir/my_dir && chown 1000:root /workdir/my_dir'
    )
    copy_req_idx = steps.index(
        'COPY --chown=1000:root my_dir/requirements.txt'
        ' /workdir/my_dir/requirements.txt'
    )
    self.assertLess(pip_upgrade_idx, mkdir_idx)
    self.assertLess(mkdir_idx, copy_req_idx)
    self.assertIn('COPY --chown=1000:root my_dir/ /workdir/my_dir', steps)

  def test_create_entrypoint_permissions(self):
    project = self.create_container(xm.ModuleName('some.module'))
    entrypoint_path = build_image._create_entrypoint(project)
    try:
      self.assertEqual(os.stat(entrypoint_path).st_mode & 0o777, 0o775)
    finally:
      os.remove(entrypoint_path)

  def test_prepare_directory_sets_permissions(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      src_dir = os.path.join(temp_dir, 'src')
      dest_dir = os.path.join(temp_dir, 'dest')
      os.makedirs(os.path.join(src_dir, 'subdir'))
      file_path = os.path.join(src_dir, 'subdir', 'foo.py')
      with open(file_path, 'w') as f:
        f.write('print("hello")')
      os.chmod(file_path, 0o644)
      os.chmod(os.path.join(src_dir, 'subdir'), 0o755)

      entrypoint_file = os.path.join(temp_dir, 'entrypoint.sh')
      with open(entrypoint_file, 'w') as f:
        f.write('#!/bin/bash\necho test')
      os.chmod(entrypoint_file, 0o644)

      dockerfile = os.path.join(temp_dir, 'Dockerfile')
      with open(dockerfile, 'w') as f:
        f.write('FROM ubuntu:latest')

      docker_lib.prepare_directory(
          destination_directory=dest_dir,
          source_directory=src_dir,
          project_name='project',
          entrypoint_file=entrypoint_file,
          dockerfile=dockerfile,
      )

      staged_project_dir = os.path.join(dest_dir, 'project')
      self.assertEqual(os.stat(staged_project_dir).st_mode & 0o777, 0o775)
      staged_subdir = os.path.join(staged_project_dir, 'subdir')
      self.assertEqual(os.stat(staged_subdir).st_mode & 0o777, 0o775)
      staged_file = os.path.join(staged_subdir, 'foo.py')
      self.assertEqual(os.stat(staged_file).st_mode & 0o777, 0o775)
      staged_entrypoint = os.path.join(dest_dir, 'entrypoint.sh')
      self.assertEqual(os.stat(staged_entrypoint).st_mode & 0o777, 0o775)
      self.assertTrue(os.path.exists(os.path.join(dest_dir, 'Dockerfile')))

  def test_build_image_with_docker_command_progress(self):
    mock_client = mock.MagicMock()
    mock_client.version.return_value = {'Version': '24.0.0'}
    with mock.patch.object(docker_lib.subprocess, 'run') as mock_run:
      docker_lib._build_image_with_docker_command(
          mock_client,
          path='/fake/path',
          image_tag='gcr.io/test/image:tag',
          dockerfile='/fake/path/Dockerfile',
          progress=True,
      )
      mock_run.assert_called_once()
      command = mock_run.call_args[0][0]
      self.assertEqual(
          command[0:5],
          ['docker', 'buildx', 'build', '--progress', 'plain'],
      )
      self.assertNotIn('--no-cache', command)

  def test_build_image_with_docker_command_no_progress(self):
    mock_client = mock.MagicMock()
    mock_client.version.return_value = {'Version': '24.0.0'}
    with mock.patch.object(docker_lib.subprocess, 'run') as mock_run:
      docker_lib._build_image_with_docker_command(
          mock_client,
          path='/fake/path',
          image_tag='gcr.io/test/image:tag',
          dockerfile='/fake/path/Dockerfile',
          progress=False,
      )
      mock_run.assert_called_once()
      command = mock_run.call_args[0][0]
      self.assertEqual(command[0:3], ['docker', 'buildx', 'build'])
      self.assertNotIn('--progress', command)
      self.assertNotIn('--no-cache', command)

  def test_push_docker_image_cold_push_extracts_digest_from_stream(self):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    mock_image.attrs = {'RepoDigests': []}
    mock_client.images.get.return_value = mock_image
    mock_client.images.push.return_value = (
        '{"status":"Pushing..."}\n{"aux":{"Digest":"sha256:abcd1234ef"}}\n'
    )

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      result = docker_lib.push_docker_image('gcr.io/test/image:v1')

    self.assertEqual(result, 'gcr.io/test/image@sha256:abcd1234ef')
    mock_client.images.push.assert_called_once_with(
        repository='gcr.io/test/image', tag='latest'
    )
    mock_client.api.inspect_distribution.assert_not_called()

  def test_push_docker_image_extracts_digest_from_inspect_distribution(self):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    mock_image.attrs = {'RepoDigests': []}
    mock_client.images.get.return_value = mock_image
    mock_client.images.push.return_value = '{"status":"Pushing..."}\n'
    mock_client.api.inspect_distribution.return_value = {
        'Descriptor': {'digest': 'sha256:distdigest1234'}
    }

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      result = docker_lib.push_docker_image('gcr.io/test/image:v1')

    self.assertEqual(result, 'gcr.io/test/image@sha256:distdigest1234')
    mock_client.images.push.assert_called_once_with(
        repository='gcr.io/test/image', tag='latest'
    )
    mock_client.api.inspect_distribution.assert_called_once_with(
        'gcr.io/test/image:latest'
    )

  def test_push_docker_image_falls_back_to_tag_when_no_digest_available(self):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    mock_image.attrs = {'RepoDigests': []}
    mock_client.images.get.return_value = mock_image
    mock_client.images.push.return_value = '{"status":"Pushing..."}\n'
    mock_client.api.inspect_distribution.return_value = {}

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      result = docker_lib.push_docker_image('gcr.io/test/image:v1')

    self.assertEqual(result, 'gcr.io/test/image:v1')
    mock_client.images.push.assert_called_once_with(
        repository='gcr.io/test/image', tag='latest'
    )

  def test_push_docker_image_warm_push_short_circuit(self):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    existing_repo_digest = 'gcr.io/test/image@sha256:existing1234'
    mock_image.attrs = {'RepoDigests': [existing_repo_digest]}
    mock_client.images.get.return_value = mock_image
    mock_client.api.inspect_distribution.return_value = {}

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      result = docker_lib.push_docker_image('gcr.io/test/image:v1')

    self.assertEqual(result, existing_repo_digest)
    mock_client.images.push.assert_not_called()
    mock_client.api.inspect_distribution.assert_called_once_with(
        existing_repo_digest
    )

  def test_push_docker_image_warm_push_falls_back_when_remote_digest_missing(
      self,
  ):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    mock_image.attrs = {'RepoDigests': ['gcr.io/test/image@sha256:stale1234']}
    mock_client.images.get.return_value = mock_image
    mock_client.api.inspect_distribution.side_effect = docker.errors.APIError(
        '404 Not Found'
    )
    mock_client.images.push.return_value = (
        '{"aux":{"Digest":"sha256:fresh5678"}}\n'
    )

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      result = docker_lib.push_docker_image('gcr.io/test/image:v1')

    self.assertEqual(result, 'gcr.io/test/image@sha256:fresh5678')
    mock_client.images.push.assert_called_once_with(
        repository='gcr.io/test/image', tag='latest'
    )

  def test_push_docker_image_raises_on_stream_error(self):
    mock_client = mock.MagicMock()
    mock_image = mock.MagicMock()
    mock_image.attrs = {'RepoDigests': []}
    mock_client.images.get.return_value = mock_image
    mock_client.images.push.return_value = (
        '{"error":"denied","errorDetail":{"message":"access denied"}}\n'
    )

    with mock.patch.object(docker, 'from_env', return_value=mock_client):
      with self.assertRaisesRegex(RuntimeError, 'access denied'):
        docker_lib.push_docker_image('gcr.io/test/image:v1')


if __name__ == '__main__':
  absltest.main()
