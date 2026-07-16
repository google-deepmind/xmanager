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

import functools
from typing import Sequence
import unittest
from unittest import mock

from absl import flags
from xmanager.xm_local.packaging import bazel_tools

from xmanager.generated import build_event_stream_pb2 as bes_pb2


def _file(name: str, uri: str) -> bes_pb2.File:
  return bes_pb2.File(name=name, uri=uri)


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


def _output_group(name: str, file_set_ids: list[str]) -> bes_pb2.OutputGroup:
  return bes_pb2.OutputGroup(
      name=name,
      file_sets=[
          bes_pb2.BuildEventId.NamedSetOfFilesId(id=file_set_id)
          for file_set_id in file_set_ids
      ],
  )


def _target_completed_event(
    label: str,
    important_output: Sequence[bes_pb2.File] = (),
    output_groups: Sequence[bes_pb2.OutputGroup] = (),
) -> bes_pb2.BuildEvent:
  return bes_pb2.BuildEvent(
      id=bes_pb2.BuildEventId(
          target_completed=bes_pb2.BuildEventId.TargetCompletedId(label=label)
      ),
      completed=bes_pb2.TargetComplete(
          success=True,
          important_output=important_output,
          output_group=output_groups,
      ),
  )


def _fail_exec_root() -> str:
  raise AssertionError('exec_root should not be queried for `file://` URIs')


class BazelToolsTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()

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
    binary = _file('bin', 'file:///root/bin')
    events = [_target_completed_event('//:bin', important_output=[binary])]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_from_named_set(self):
    binary = _file('bin', 'file:///root/bin')
    events = [
        _named_set_event('0', [binary]),
        _target_completed_event(
            '//:bin', output_groups=[_output_group('default', ['0'])]
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_ignores_non_default_groups(self):
    binary = _file('bin', 'file:///root/bin')
    binary_zip = _file('bin.zip', 'file:///root/bin.zip')
    events = [
        _named_set_event('0', [binary]),
        _named_set_event('1', [binary_zip]),
        _target_completed_event(
            '//:bin',
            output_groups=[
                _output_group('default', ['0']),
                _output_group('python_zip_file', ['1']),
            ],
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_prefers_important_output(self):
    binary = _file('bin', 'file:///root/bin')
    other = _file('other', 'file:///root/other')
    events = [
        _named_set_event('0', [other]),
        _target_completed_event(
            '//:bin',
            important_output=[binary],
            output_groups=[_output_group('default', ['0'])],
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']), [[binary]]
    )

  def test_get_important_outputs_resolves_nested_file_sets(self):
    first = _file('first', 'file:///root/first')
    second = _file('second', 'file:///root/second')
    events = [
        _named_set_event('0', [first], nested_ids=['1']),
        _named_set_event('1', [second]),
        _target_completed_event(
            '//:bin', output_groups=[_output_group('default', ['0'])]
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']),
        [[first, second]],
    )

  def test_get_important_outputs_handles_cyclic_file_sets(self):
    first = _file('first', 'file:///root/first')
    second = _file('second', 'file:///root/second')
    events = [
        _named_set_event('0', [first], nested_ids=['1']),
        _named_set_event('1', [second], nested_ids=['0']),
        _target_completed_event(
            '//:bin', output_groups=[_output_group('default', ['0'])]
        ),
    ]
    self.assertEqual(
        bazel_tools._get_important_outputs(events, ['//:bin']),
        [[first, second]],
    )

  def test_get_important_outputs_missing_label_raises(self):
    events = [_named_set_event('0', [_file('bin', 'file:///root/bin')])]
    with self.assertRaises(KeyError):
      bazel_tools._get_important_outputs(events, ['//:missing'])

  def test_resolve_output_path_prefers_file_uri(self):
    # A `file://` URI is the authoritative on-disk path and must be used
    # without querying the execution root.
    file = bes_pb2.File(
        uri='file:///output_base/execroot/ws/bazel-out/bin/foo',
        name='foo',
        path_prefix=['bazel-out', 'bin'],
    )
    self.assertEqual(
        bazel_tools._resolve_output_path(file, _fail_exec_root),
        '/output_base/execroot/ws/bazel-out/bin/foo',
    )

  def test_resolve_output_path_decodes_percent_encoding(self):
    file = bes_pb2.File(uri='file:///tmp/out%20dir/foo%2Bbar')
    self.assertEqual(
        bazel_tools._resolve_output_path(file, _fail_exec_root),
        '/tmp/out dir/foo+bar',
    )

  def test_resolve_output_path_preserves_uri_authority(self):
    file = bes_pb2.File(uri='file://host/share/foo')
    self.assertEqual(
        bazel_tools._resolve_output_path(file, _fail_exec_root),
        '//host/share/foo',
    )

  def test_resolve_output_path_ignores_localhost_authority(self):
    file = bes_pb2.File(uri='file://localhost/tmp/foo')
    self.assertEqual(
        bazel_tools._resolve_output_path(file, _fail_exec_root),
        '/tmp/foo',
    )

  def test_resolve_output_path_falls_back_for_non_file_uri(self):
    # Remote outputs (e.g. `bytestream://`) have no usable `file://` URI, so we
    # join the execution root with the workspace-relative `path_prefix`.
    file = bes_pb2.File(
        uri='bytestream://remote.example.com/blobs/abc/123',
        name='foo',
        path_prefix=['bazel-out', 'k8-fastbuild', 'bin'],
    )
    self.assertEqual(
        bazel_tools._resolve_output_path(file, lambda: '/exec/root'),
        '/exec/root/bazel-out/k8-fastbuild/bin/foo',
    )

  def test_resolve_output_path_falls_back_for_missing_uri(self):
    # The `uri` field is unset (e.g. inlined `contents`); fall back to the
    # `path_prefix`.
    file = bes_pb2.File(name='foo', path_prefix=['bazel-out', 'bin'])
    self.assertEqual(
        bazel_tools._resolve_output_path(file, lambda: '/exec/root'),
        '/exec/root/bazel-out/bin/foo',
    )

  def test_resolve_output_path_queries_exec_root_at_most_once(self):
    # Mirrors the wrapping in `_build_multiple_targets`: a single
    # `lru_cache`-wrapped callable shared across all outputs must shell out to
    # `bazel info` at most once, even when several outputs take the fallback.
    calls = 0

    def _counting_exec_root() -> str:
      nonlocal calls
      calls += 1
      return '/exec/root'

    exec_root_fn = functools.lru_cache(_counting_exec_root)
    files = [
        bes_pb2.File(
            uri='file:///exec/root/bazel-out/bin/local',
            name='local',
            path_prefix=['bazel-out', 'bin'],
        ),
        bes_pb2.File(
            uri='bytestream://remote.example.com/blobs/abc/1',
            name='remote_a',
            path_prefix=['bazel-out', 'bin'],
        ),
        bes_pb2.File(
            uri='bytestream://remote.example.com/blobs/abc/2',
            name='remote_b',
            path_prefix=['bazel-out', 'bin'],
        ),
    ]

    paths = [bazel_tools._resolve_output_path(f, exec_root_fn) for f in files]

    self.assertEqual(
        paths,
        [
            '/exec/root/bazel-out/bin/local',
            '/exec/root/bazel-out/bin/remote_a',
            '/exec/root/bazel-out/bin/remote_b',
        ],
    )
    self.assertEqual(calls, 1)

  def test_resolve_output_path_never_queries_exec_root_for_file_uris(self):
    # When every output has a `file://` URI the exec root is never queried.
    calls = 0

    def _counting_exec_root() -> str:
      nonlocal calls
      calls += 1
      return '/exec/root'

    exec_root_fn = functools.lru_cache(_counting_exec_root)
    files = [
        bes_pb2.File(uri='file:///exec/root/bazel-out/bin/a', name='a'),
        bes_pb2.File(uri='file:///exec/root/bazel-out/bin/b', name='b'),
    ]

    _ = [bazel_tools._resolve_output_path(f, exec_root_fn) for f in files]

    self.assertEqual(calls, 0)

  def test_execution_root_queries_bazel(self):
    completed = mock.Mock(stdout='/mock/execroot\n')
    with mock.patch.object(
        bazel_tools, '_root_absolute_path', return_value='/workspace'
    ), mock.patch.object(
        bazel_tools.subprocess, 'run', return_value=completed
    ) as run:
      result = bazel_tools._execution_root()

    self.assertEqual(result, '/mock/execroot')
    run.assert_called_once()
    args, kwargs = run.call_args
    self.assertEqual(args[0][1:], ['info', 'execution_root'])
    self.assertEqual(kwargs['cwd'], '/workspace')
    self.assertTrue(kwargs['check'])

  def test_build_multiple_targets_resolves_paths(self):
    binary = _file('bin', 'file:///root/bazel-out/bin')
    remote_file = _file('remote', 'bytestream://remote/123')
    remote_file.path_prefix.extend(['bazel-out', 'bin'])
    events = [
        _target_completed_event('//:t1', important_output=[binary]),
        _target_completed_event('//:t2', important_output=[remote_file]),
    ]
    completed = mock.Mock()
    with mock.patch.object(
        bazel_tools.file_utils, 'TemporaryFilePath'
    ), mock.patch.object(
        bazel_tools.subprocess, 'run', return_value=completed
    ), mock.patch.object(
        bazel_tools, '_read_build_events', return_value=events
    ), mock.patch.object(
        bazel_tools, '_get_normalized_labels', return_value=['//:t1', '//:t2']
    ), mock.patch.object(
        bazel_tools, '_root_absolute_path', return_value='/workspace'
    ), mock.patch.object(
        bazel_tools, '_execution_root', return_value='/mock/execroot'
    ):
      paths = bazel_tools._build_multiple_targets(['//:t1', '//:t2'])

    self.assertEqual(
        paths,
        [['/root/bazel-out/bin'], ['/mock/execroot/bazel-out/bin/remote']],
    )


class QueryExecutableOutputTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    # `query_executable_output` reads `xm_flags.BAZEL_COMMAND.value`, which
    # requires flags to have been parsed (absl raises otherwise). The default
    # (`bazel`) is sufficient here since the command is mocked.
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()

  def test_builds_cquery_command_and_returns_path(self):
    completed = mock.Mock(stdout='/x/bar\n')
    with mock.patch.object(
        bazel_tools, '_root_absolute_path', return_value='/root'
    ), mock.patch.object(
        bazel_tools.subprocess, 'run', return_value=completed
    ) as run:
      result = bazel_tools.query_executable_output('//foo:bar', ('-c', 'opt'))

    self.assertEqual(result, '/x/bar')
    run.assert_called_once()
    args, kwargs = run.call_args
    command = args[0]
    self.assertIn('cquery', command)
    self.assertIn('//foo:bar', command)
    self.assertIn('-c', command)
    self.assertIn('opt', command)
    self.assertIn('--output=starlark', command)
    self.assertTrue(
        any(arg.startswith('--starlark:expr=') for arg in command),
        command,
    )
    self.assertEqual(kwargs['cwd'], '/root')
    self.assertTrue(kwargs['check'])

  def test_empty_stdout_returns_none(self):
    completed = mock.Mock(stdout='\n')
    with mock.patch.object(
        bazel_tools, '_root_absolute_path', return_value='/root'
    ), mock.patch.object(bazel_tools.subprocess, 'run', return_value=completed):
      self.assertIsNone(bazel_tools.query_executable_output('//foo:bar'))


if __name__ == '__main__':
  unittest.main()
