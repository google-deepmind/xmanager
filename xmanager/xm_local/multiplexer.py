"""Methods for running processes in a terminal multiplexer."""

import asyncio
import functools
import logging
import shutil
import subprocess

from xmanager import xm_flags


def _has_tmux() -> bool:
  """Checks whether tmux is installed."""
  return shutil.which('tmux') is not None


def _get_executable_command(
    executable_path: str, args: list[str], env_vars: dict[str, str]
) -> str:
  """Builds a command to run the given executable with args and envs."""
  env_as_list = [f'{k}={v}' for k, v in env_vars.items()]
  launch_command = subprocess.list2cmdline(
      [*env_as_list, executable_path, *args]
  )
  # When the command is done, echo the command so it can be copy-pasted, and
  # then drop into a shell.
  command = (
      f'{launch_command}; '
      f'echo; echo Job completed.; echo {launch_command}; exec $SHELL'
  )
  return command


class Multiplexer:
  """Manages running processes in a terminal multiplexer."""

  def __init__(self):
    if not _has_tmux():
      raise ValueError('tmux must be installed')
    self._session_name = None
    self._window_count = 0

  async def _new_session(
      self,
      inner_command: str,
      full_job_name: str,
  ) -> asyncio.subprocess.Process:
    """Starts a new tmux session with the specified executable."""
    session_name_prefix = 'xm'
    session_name_suffix = 0
    self._session_name = f'{session_name_prefix}_{session_name_suffix}'

    while True:
      process = await asyncio.create_subprocess_shell(
          subprocess.list2cmdline([
              'tmux',
              'new-session',
              '-d',
              '-s',
              self._session_name,
              '-n',
              full_job_name,
              inner_command,
          ]),
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )
      # Check for errors creating the session.
      returncode = await process.wait()
      if returncode == 0:
        break
      assert process.stderr
      stderr = await process.stderr.read()
      if 'duplicate session' in stderr.decode():
        logging.info(
            'tmux session %s exists, trying a unique session name...',
            self._session_name,
        )
        session_name_suffix += 1
        self._session_name = f'{session_name_prefix}_{session_name_suffix}'
      else:
        raise ValueError(f'Failed to create tmux session: {stderr}')

    print(
        f'Created new tmux session called "{self._session_name}". If you are'
        ' already in a tmux session, use `Ctrl+B W` as a convenient way to'
        ' switch to the new session. Otherwise run \n\n  tmux a -t'
        f' "{self._session_name}"\n\nYou can also set'
        f' --{xm_flags.OPEN_MULTIPLEXER_WINDOW.name}=<index starting from 1> '
        'to automatically switch to a window in the new session.'
    )
    # Note: the process returned here corresponds to the tmux window, not the
    # command running inside.
    return process

  async def add(
      self,
      executable_path: str,
      args: list[str],
      env_vars: dict[str, str],
      full_job_name: str,
  ) -> asyncio.subprocess.Process:
    """Runs the given command in a new window."""
    inner_command = _get_executable_command(executable_path, args, env_vars)

    # New session automatically creates a window, so we delay creating the
    # session until the first process is added.
    if not self._session_name:
      process = await self._new_session(inner_command, full_job_name)
    else:
      process = await asyncio.create_subprocess_shell(
          subprocess.list2cmdline([
              'tmux',
              'new-window',
              '-t',
              self._session_name,
              '-n',
              full_job_name,
              inner_command,
          ]),
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
      )

    self._window_count += 1
    if self._window_count == xm_flags.OPEN_MULTIPLEXER_WINDOW.value:
      target = f'{self._session_name}:{xm_flags.OPEN_MULTIPLEXER_WINDOW.value}'
      subprocess.run(['tmux', 'switch-client', '-t', target], check=True)

    return process


@functools.cache
def instance() -> Multiplexer:
  """Returns the existing multiplexer or creates a new one."""
  return Multiplexer()
