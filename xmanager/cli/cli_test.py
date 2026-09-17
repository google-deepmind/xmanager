# Copyright 2026 DeepMind Technologies Limited
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
"""Tests for XManager CLI launch command."""

from collections.abc import Sequence
import subprocess
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from xmanager.cli import cli


class CliLaunchTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='with_separator_splits_bazel_and_script_flags',
          target='//foo/bar:launcher',
          tail_argv=['-c', 'opt', '--config=cuda', '--', '--lr=0.01'],
          expected_bazel_flags=['-c', 'opt', '--config=cuda'],
          expected_script_args=['--lr=0.01'],
      ),
      dict(
          testcase_name='without_separator_passes_all_as_bazel_flags',
          target=':local_launcher',
          tail_argv=['-c', 'opt'],
          expected_bazel_flags=['-c', 'opt'],
          expected_script_args=[],
      ),
  )
  def test_launch_bazel_target_invokes_bazel_run(
      self,
      target: str,
      tail_argv: Sequence[str],
      expected_bazel_flags: list[str],
      expected_script_args: list[str],
  ) -> None:
    completed_run = subprocess.CompletedProcess(args=[], returncode=42)

    with (
        mock.patch.object(
            subprocess, 'run', return_value=completed_run
        ) as mock_run,
        mock.patch.object(cli.sys, 'exit') as mock_exit,
    ):
      parsed_argv = cli._parse_flags(['xmanager', 'launch', target, *tail_argv])
      cli.main(parsed_argv)

    mock_run.assert_called_once_with(
        [
            'bazel',
            'run',
            *expected_bazel_flags,
            target,
            '--',
            f'--xm_launch_script={target}',
            *expected_script_args,
        ],
        check=False,
    )
    mock_exit.assert_called_once_with(42)

  def test_launch_bazel_target_warns_when_flags_passed_without_separator(
      self,
  ) -> None:
    completed_run = subprocess.CompletedProcess(args=[], returncode=0)

    with (
        mock.patch.object(subprocess, 'run', return_value=completed_run),
        mock.patch.object(cli.sys, 'exit'),
        self.assertLogs(level='WARNING') as logs,
    ):
      parsed_argv = cli._parse_flags(
          ['xmanager', 'launch', '//foo:launcher', '--param1=foo']
      )
      cli.main(parsed_argv)

    self.assertTrue(
        any(
            'xmanager launch //foo:launcher -- --param1=foo' in msg
            for msg in logs.output
        ),
        msg=f'Expected separator hint in warning logs, got: {logs.output}',
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='without_separator',
          tail_argv=['--lr=0.01', '--batch_size=32'],
      ),
      dict(
          testcase_name='with_separator',
          tail_argv=['--', '--lr=0.01', '--batch_size=32'],
      ),
  )
  def test_launch_python_script_forwards_script_flags(
      self,
      tail_argv: Sequence[str],
  ) -> None:
    script_file = self.create_tempfile(
        'my_launcher.py', content='def main(argv):\n  pass\n'
    )
    script_path = script_file.full_path

    with mock.patch.object(cli.app, 'run') as mock_app_run:
      parsed_argv = cli._parse_flags(
          ['xmanager', 'launch', script_path, *tail_argv]
      )
      cli.main(parsed_argv)

    mock_app_run.assert_called_once_with(
        mock.ANY,
        argv=[
            script_path,
            f'--xm_launch_script={script_path}',
            '--lr=0.01',
            '--batch_size=32',
        ],
    )


if __name__ == '__main__':
  absltest.main()
