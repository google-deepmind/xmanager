"""Methods for running processes in a terminal multiplexer."""

import asyncio
import collections
import functools
import logging
import os
import shutil
import subprocess


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
  command = f'{launch_command}; echo {launch_command}; exec $SHELL'
  return command


class Multiplexer:
  """Manages running processes in a terminal multiplexer."""

  def __init__(self):
    if not _has_tmux():
      raise ValueError('tmux must be installed')
    self._session_name = None
    self._window_names = collections.Counter()

  def _get_window_name(self, executable_path: str) -> str:
    """Gets a unique window name for the given executable."""
    window_name = os.path.basename(executable_path)
    self._window_names[window_name] += 1
    if self._window_names[window_name] > 1:
      count = self._window_names[window_name]
      window_name += f'_{count}'
    return window_name

  async def _new_session(
      self, executable_path: str, args: list[str], env_vars: dict[str, str]
  ) -> asyncio.subprocess.Process:
    """Starts a new tmux session with the specified executable."""
    session_name_prefix = 'xm'
    session_name_suffix = 0
    self._session_name = f'{session_name_prefix}_{session_name_suffix}'
    inner_command = _get_executable_command(executable_path, args, env_vars)
    window_name = self._get_window_name(executable_path)

    while True:
      process = await asyncio.create_subprocess_shell(
          subprocess.list2cmdline([
              'tmux',
              'new-session',
              '-d',
              '-s',
              self._session_name,
              '-n',
              window_name,
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
        f'Created new tmux session called "{self._session_name}". '
        'If you are already in a tmux session, use `Ctrl+B W` as a '
        'convenient way to switch to the new session. '
        f'Otherwise run \n\n  tmux a -t "{self._session_name}"'
    )
    # Note: the process returned here corresponds to the tmux window, not the
    # command running inside.
    return process

  async def add(
      self, executable_path: str, args: list[str], env_vars: dict[str, str]
  ) -> asyncio.subprocess.Process:
    """Runs the given command in a new window."""
    # New session automatically creates a window, so we delay creating the
    # session until the first process is added.
    if not self._session_name:
      return await self._new_session(executable_path, args, env_vars)

    window_name = self._get_window_name(executable_path)
    inner_command = _get_executable_command(executable_path, args, env_vars)
    command_list = [
        'tmux',
        'new-window',
        '-t',
        self._session_name,
        '-n',
        window_name,
        inner_command,
    ]
    return await asyncio.create_subprocess_shell(
        subprocess.list2cmdline(command_list),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


@functools.cache
def instance() -> Multiplexer:
  """Returns the existing multiplexer or creates a new one."""
  return Multiplexer()
