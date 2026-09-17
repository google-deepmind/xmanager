"""Tests for xmanager.xm_local.multiplexer."""

import json
import os
import shlex
import subprocess
import sys
import unittest
from unittest import mock

from absl import flags
from xmanager import xm
from xmanager.xm import utils
from xmanager.xm_local import multiplexer

_SCRIPT = (
    'import json, os, sys; '
    'print(json.dumps([sys.argv[1], os.environ["TEST_VALUE"]]))'
)
_EXPECTED_OUTPUT = ['{"d": 4}', 'with space']

# `execution.py` merges the whole ambient environment into `env_vars`, so an
# interactive shell's values reach the multiplexer as they are.
_METACHARACTER_ENV = {
    'LS_COLORS': 'di=1;36:ln=35:*.tar=01;31',
    'FZF_DEFAULT_OPTS': "--height 40%\n--bind '?:toggle-preview'",
    'LESS_TERMCAP_md': '\x1b[1m\x1b[32m',
}


def _executable_command() -> str:
  """Builds a command whose argument and environment both need quoting."""
  args = xm.SequentialArgs.from_collection([
      '-c',
      _SCRIPT,
      '{"d": 4}',
  ]).to_list(utils.ARG_ESCAPER)
  return multiplexer._get_executable_command(
      sys.executable,
      args,
      {'TEST_VALUE': 'with space'},
  )


def _run(command: str) -> subprocess.CompletedProcess:
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


class MultiplexerTest(unittest.IsolatedAsyncioTestCase):

  def setUp(self):
    super().setUp()
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()

  def test_get_executable_command_preserves_arguments_and_environment(self):
    result = _run(_executable_command())

    self.assertEqual(
        json.loads(result.stdout.splitlines()[0]), _EXPECTED_OUTPUT
    )

  def test_get_executable_command_echoes_a_reusable_command(self):
    result = _run(_executable_command())

    echoed_command = result.stdout.splitlines()[-1]
    rerun = _run(echoed_command)
    self.assertEqual(
        json.loads(rerun.stdout.splitlines()[0]), _EXPECTED_OUTPUT
    )

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
  unittest.main()
