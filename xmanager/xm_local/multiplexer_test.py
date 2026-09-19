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
import shlex
import subprocess
import sys
import unittest
from unittest import mock

from absl import flags
from absl.testing import absltest
from xmanager import xm
from xmanager.xm import utils
from xmanager.xm_local import multiplexer


# `execution.py` merges the whole ambient environment into `env_vars`, so an
# interactive shell's values reach the multiplexer as they are.
_METACHARACTER_ENV = {
    'LS_COLORS': 'di=1;36:ln=35:*.tar=01;31',
    'FZF_DEFAULT_OPTS': "--height 40%\n--bind '?:toggle-preview'",
    'LESS_TERMCAP_md': '\x1b[1m\x1b[32m',
}


def _executable_command(script_path: str) -> str:
  """Builds a command whose argument and environment both need quoting."""
  args = xm.SequentialArgs.from_collection([
      '{"d": 4}',
  ]).to_list(utils.ARG_ESCAPER)
  return multiplexer._get_executable_command(
      script_path,
      args,
      {'TEST_VALUE': 'with space'},
  )


def _run(command: str) -> subprocess.CompletedProcess[str]:
  return subprocess.run(
      command,
      shell=True,
      check=True,
      capture_output=True,
      text=True,
      # An empty `SHELL` turns the trailing `exec $SHELL` into a no-op, so the
      # command terminates instead of waiting for an interactive shell.
      env={**os.environ, 'SHELL': ''},
  )


class MultiplexerTest(absltest.TestCase, unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    super().setUp()
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()
    self._test_script = os.path.join(
        self.create_tempdir().full_path, 'test_script.sh'
    )
    with open(self._test_script, 'w') as file:
      file.write('#!/bin/sh\nprintf "%s\\n%s\\n" "$1" "$TEST_VALUE"\n')
    os.chmod(self._test_script, 0o755)

  def test_get_executable_command_preserves_arguments_and_environment(self):
    result = _run(_executable_command(self._test_script))

    lines = result.stdout.splitlines()
    self.assertEqual(lines[:2], ['{"d": 4}', 'with space'])

  def test_get_executable_command_echoes_a_reusable_command(self):
    result = _run(_executable_command(self._test_script))

    echoed_command = result.stdout.splitlines()[-1]
    rerun = _run(echoed_command)
    lines = rerun.stdout.splitlines()
    self.assertEqual(lines[:2], ['{"d": 4}', 'with space'])

  def test_get_executable_command_expands_job_environment(self):
    args = xm.SequentialArgs.from_collection([
        '-c',
        'import sys; print(sys.argv[1])',
        utils.ShellSafeArg('$TEST_VALUE'),
    ]).to_list(utils.ARG_ESCAPER)
    command = multiplexer._get_executable_command(
        sys.executable,
        args,
        {'TEST_VALUE': 'expanded'},
    )

    result = _run(command)

    output = result.stdout.splitlines()
    self.assertEqual(output[0], 'expanded')
    self.assertEqual(
        output[-1],
        'export TEST_VALUE=expanded; '
        + ' '.join([shlex.quote(sys.executable), *args]),
    )

  def test_get_executable_command_quotes_environment_metacharacters(self):
    script = (
        'import json, os, sys; '
        'print(json.dumps({name: os.environ[name] for name in sys.argv[1:]}))'
    )
    args = xm.SequentialArgs.from_collection(
        ['-c', script, *_METACHARACTER_ENV]
    ).to_list(utils.ARG_ESCAPER)
    command = multiplexer._get_executable_command(
        sys.executable,
        args,
        _METACHARACTER_ENV,
    )

    result = _run(command)

    self.assertEqual(
        json.loads(result.stdout.splitlines()[0]), _METACHARACTER_ENV
    )

  @mock.patch.object(multiplexer, '_has_tmux', return_value=True)
  @mock.patch.object(multiplexer.asyncio, 'create_subprocess_shell')
  async def test_new_session_quotes_tmux_arguments(
      self, create_subprocess_shell, unused_has_tmux
  ):
    process = mock.AsyncMock()
    process.wait.return_value = 0
    create_subprocess_shell.return_value = process
    tmux = multiplexer.Multiplexer()

    await tmux._new_session("printf '%s\\n' 'a b'", 'job with space')

    command = create_subprocess_shell.call_args.args[0]
    self.assertEqual(
        shlex.split(command),
        [
            'tmux',
            'new-session',
            '-d',
            '-s',
            'xm_0',
            '-n',
            'job with space',
            "printf '%s\\n' 'a b'",
        ],
    )

  @mock.patch.object(multiplexer, '_has_tmux', return_value=True)
  @mock.patch.object(multiplexer.asyncio, 'create_subprocess_shell')
  async def test_add_quotes_tmux_arguments_of_a_further_window(
      self, create_subprocess_shell, unused_has_tmux
  ):
    process = mock.AsyncMock()
    process.wait.return_value = 0
    create_subprocess_shell.return_value = process
    tmux = multiplexer.Multiplexer()
    await tmux.add('/bin/echo', [], {}, 'first job')

    await tmux.add('/bin/echo', [], {}, 'job with space')

    command = create_subprocess_shell.call_args.args[0]
    self.assertEqual(
        shlex.split(command)[:6],
        ['tmux', 'new-window', '-t', 'xm_0', '-n', 'job with space'],
    )


if __name__ == '__main__':
  absltest.main()
