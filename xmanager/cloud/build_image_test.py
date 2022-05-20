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
from absl.testing import absltest
from xmanager import xm
from xmanager.cloud import build_image


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


if __name__ == '__main__':
  absltest.main()
