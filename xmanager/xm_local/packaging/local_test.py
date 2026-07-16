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
"""Unit tests for local packaging executable selection and binary packaging."""

from unittest import mock

from absl import flags
from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local.packaging import bazel_tools
from xmanager.xm_local.packaging import local

from absl.testing import absltest as googletest


class SelectExecutableTest(googletest.TestCase):

  def test_single_output_returned_without_query(self):
    # The fast path must not shell out to Bazel at all.
    with mock.patch.object(bazel_tools, 'query_executable_output') as query:
      result = local._select_executable('//foo:bar', (), ['/out/bar'])

    self.assertEqual(result, '/out/bar')
    query.assert_not_called()

  def test_multiple_outputs_selects_matching_executable(self):
    # The launcher is deliberately not first, so a naive `paths[0]` (the
    # pre-fix behavior) would return the wrong file and fail this test.
    paths = ['/out/bar.py', '/out/__init__.py', '/out/bar']
    with mock.patch.object(
        bazel_tools, 'query_executable_output', return_value='/x/bar'
    ) as query:
      result = local._select_executable('//foo:bar', ('-c', 'opt'), paths)

    self.assertEqual(result, '/out/bar')
    # `bazel_args` must be forwarded so `cquery` resolves the same config.
    query.assert_called_once_with('//foo:bar', ('-c', 'opt'))

  def test_non_executable_target_raises(self):
    with mock.patch.object(
        bazel_tools, 'query_executable_output', return_value=None
    ):
      with self.assertRaisesRegex(ValueError, 'not an executable target'):
        local._select_executable('//foo:lib', (), ['/out/a', '/out/b'])

  def test_no_basename_match_raises(self):
    with mock.patch.object(
        bazel_tools, 'query_executable_output', return_value='/x/nope'
    ):
      with self.assertRaisesRegex(ValueError, 'Could not identify'):
        local._select_executable('//foo:bar', (), ['/out/a', '/out/b'])

  def test_ambiguous_basename_match_raises(self):
    # Two outputs share the executable's basename in different directories.
    paths = ['/out/a/bar', '/out/b/bar']
    with mock.patch.object(
        bazel_tools, 'query_executable_output', return_value='/x/bar'
    ):
      with self.assertRaisesRegex(ValueError, 'expected exactly one'):
        local._select_executable('//foo:bar', (), paths)


class PackageBazelBinaryTest(googletest.TestCase):

  def setUp(self):
    super().setUp()
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()

  def test_package_bazel_binary_invokes_select_executable(self):
    spec = xm.BazelBinary(label='//foo:bar', bazel_args=['--copt=-O2'])
    packageable = mock.Mock(
        executable_spec=spec,
        args=['--arg=foo'],
        env_vars={'KEY': 'VALUE'},
    )
    target = bazel_client.BazelTarget(
        label='//foo:bar',
        bazel_args=bazel_tools.apply_default_bazel_args(spec.bazel_args),
    )
    paths = ['/out/bar.py', '/out/bar']
    bazel_outputs = {target: paths}

    with mock.patch.object(
        local, '_select_executable', return_value='/out/bar'
    ) as select_mock:
      executable = local._package_bazel_binary(bazel_outputs, packageable, spec)

    self.assertIsInstance(executable, local_executables.LocalBinary)
    self.assertEqual(executable.command, '/out/bar')
    self.assertEqual(executable.args, ['--arg=foo'])
    self.assertEqual(executable.env_vars, {'KEY': 'VALUE'})
    select_mock.assert_called_once_with('//foo:bar', target.bazel_args, paths)


if __name__ == '__main__':
  googletest.main()
