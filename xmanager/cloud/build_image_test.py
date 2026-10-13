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
from unittest import mock

from absl.testing import absltest
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


if __name__ == '__main__':
  absltest.main()
