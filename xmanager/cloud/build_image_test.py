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
