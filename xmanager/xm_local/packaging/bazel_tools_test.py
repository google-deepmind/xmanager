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

from typing import Sequence
import unittest

from xmanager.xm_local.packaging import bazel_tools

from xmanager.generated import build_event_stream_pb2 as bes_pb2

_BINDIR = 'bazel-out/k8-fastbuild/bin'


def _file(name: str, uri: str) -> bes_pb2.File:
  return bes_pb2.File(name=name, uri=uri)


def _configuration_event(config_id: str = 'cfg') -> bes_pb2.BuildEvent:
  return bes_pb2.BuildEvent(
      id=bes_pb2.BuildEventId(
          configuration=bes_pb2.BuildEventId.ConfigurationId(id=config_id)
      ),
      configuration=bes_pb2.Configuration(make_variable={'BINDIR': _BINDIR}),
  )


def _named_set_event(
    set_id: str,
    files: list[bes_pb2.File],
    nested_ids: Sequence[str] = (),
) -> bes_pb2.BuildEvent:
  return bes_pb2.BuildEvent(
      id=bes_pb2.BuildEventId(
          named_set=bes_pb2.BuildEventId.NamedSetOfFilesId(id=set_id)
      ),
      named_set_of_files=bes_pb2.NamedSetOfFiles(
          files=files,
          file_sets=[
              bes_pb2.BuildEventId.NamedSetOfFilesId(id=nested_id)
              for nested_id in nested_ids
          ],
      ),
  )


def _target_completed_event(
    label: str,
    important_output: list[bes_pb2.File] = (),
    file_set_ids: list[str] = (),
) -> bes_pb2.BuildEvent:
  return bes_pb2.BuildEvent(
      id=bes_pb2.BuildEventId(
          target_completed=bes_pb2.BuildEventId.TargetCompletedId(label=label)
      ),
      completed=bes_pb2.TargetComplete(
          success=True,
          important_output=important_output,
          output_group=[
              bes_pb2.OutputGroup(
                  name='default',
                  file_sets=[
                      bes_pb2.BuildEventId.NamedSetOfFilesId(id=file_set_id)
                      for file_set_id in file_set_ids
                  ],
              )
          ],
      ),
  )


class BazelToolsTest(unittest.TestCase):

  def test_lex_full_label(self):
    self.assertEqual(
        bazel_tools._lex_label('//project/directory:target'),
        (['project', 'directory'], 'target'),
    )

  def test_lex_short_label(self):
    self.assertEqual(
        bazel_tools._lex_label('//project/package'),
        (['project', 'package'], 'package'),
    )

  def test_lex_root_target(self):
    self.assertEqual(bazel_tools._lex_label('//:label'), ([], 'label'))

  def test_lex_empty_label(self):
    with self.assertRaises(ValueError):
      bazel_tools._lex_label('//')

  def test_lex_relative_label(self):
    with self.assertRaises(ValueError):
      bazel_tools._lex_label('a/b:c')

  def test_assemble_label(self):
    self.assertEqual(bazel_tools._assemble_label((['a', 'b'], 'c')), '//a/b:c')

  def test_label_kind_lines_to_dict(self):
    self.assertEqual(
        bazel_tools._label_kind_lines_to_dict([
            'py_binary rule //:py_target',
            'cc_binary rule //:cc_target',
        ]),
        {'//:py_target': 'py_binary rule', '//:cc_target': 'cc_binary rule'},
    )

  def test_absolute_label_with_extension_dot(self):
    self.assertEqual(
        bazel_tools._lex_label('//project/directory:image.tar'),
        (['project', 'directory'], 'image.tar'),
    )

  def test_label_with_three_dots(self):
    with self.assertRaisesRegex(ValueError, 'is not an absolute Bazel label'):
      bazel_tools._lex_label('//project/directory/...')

  def test_label_with_star_target(self):
    with self.assertRaisesRegex(ValueError, 'is not an absolute Bazel label'):
      bazel_tools._lex_label('//project/directory:*')

  def test_label_with_all_target(self):
    with self.assertRaisesRegex(ValueError, '`:all` is not a valid target'):
      bazel_tools._lex_label('//project/directory:all')

  def test_get_important_outputs_from_important_output(self):
    binary = _file('bin', f'file:///root/{_BINDIR}/bin')
    events = [
        _configuration_event(),
        _target_completed_event('//:bin', important_output=[binary]),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_from_named_set(self):
    binary = _file('bin', f'file:///root/{_BINDIR}/bin')
    events = [
        _configuration_event(),
        _named_set_event('0', [binary]),
        _target_completed_event('//:bin', file_set_ids=['0']),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_filters_non_bindir_files(self):
    binary = _file('bin', f'file:///root/{_BINDIR}/bin')
    source = _file('bin.py', 'file:///root/bazel-out/../bin.py')
    events = [
        _configuration_event(),
        _named_set_event('0', [binary, source]),
        _target_completed_event('//:bin', file_set_ids=['0']),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_keeps_all_without_configuration(self):
    binary = _file('bin', f'file:///root/{_BINDIR}/bin')
    source = _file('bin.py', 'file:///root/bin.py')
    events = [
        _named_set_event('0', [binary, source]),
        _target_completed_event('//:bin', file_set_ids=['0']),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']),
        [[binary, source]],
    )

  def test_get_important_outputs_resolves_nested_file_sets(self):
    first = _file('first', f'file:///root/{_BINDIR}/first')
    second = _file('second', f'file:///root/{_BINDIR}/second')
    events = [
        _configuration_event(),
        _named_set_event('0', [first], nested_ids=['1']),
        _named_set_event('1', [second]),
        _target_completed_event('//:bin', file_set_ids=['0']),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']),
        [[first, second]],
    )

  def test_get_important_outputs_handles_cyclic_file_sets(self):
    first = _file('first', f'file:///root/{_BINDIR}/first')
    second = _file('second', f'file:///root/{_BINDIR}/second')
    events = [
        _configuration_event(),
        _named_set_event('0', [first], nested_ids=['1']),
        _named_set_event('1', [second], nested_ids=['0']),
        _target_completed_event('//:bin', file_set_ids=['0']),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']),
        [[first, second]],
    )

  def test_get_important_outputs_dedupes_overlapping_files(self):
    binary = _file('bin', f'file:///root/{_BINDIR}/bin')
    events = [
        _configuration_event(),
        _named_set_event('0', [binary]),
        _target_completed_event(
            '//:bin', important_output=[binary], file_set_ids=['0']
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_missing_label_raises(self):
    events = [_configuration_event()]
    with self.assertRaises(KeyError):
      bazel_tools._get_important_outputs(events, ['//:missing'])


if __name__ == '__main__':
  unittest.main()
