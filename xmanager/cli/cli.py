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
"""XManager command-line interface."""

from collections.abc import Sequence
import errno
import importlib
import os
import shutil
import subprocess
import sys
import textwrap

from absl import app
from absl import flags
from absl import logging
from xmanager import xm_flags

_DEFAULT_ZONE = 'us-west1-b'
_DEFAULT_CLUSTER_NAME = 'xmanager-via-caliban'


def _help_command(argv):
  """Prints help message."""
  if len(argv) != 2:
    raise app.UsageError('help command takes no arguments')
  try:
    width = shutil.get_terminal_size().columns
  except OSError:
    width = 80
  print('usage: xmanager {launch,cluster} ...')
  print()
  opts = {
      'launch': 'Launches an experiment on XManager.',
      'cluster': (
         'Creates or deletes a GKE cluster for use with xm_local.'
      ),
  }
  for k, v in opts.items():
    wrapper = textwrap.TextWrapper(
        width=width, initial_indent=f'{k:<10}', subsequent_indent=' ' * 10
    )
    print(wrapper.fill(v))


def _launch_bazel_target(target: str, tail_argv: Sequence[str]) -> None:
  """Builds and runs a Bazel binary target via `bazel run`."""
  if '--' in tail_argv:
    sep = tail_argv.index('--')
    bazel_flags, script_args = tail_argv[:sep], tail_argv[sep + 1 :]
  else:
    bazel_flags, script_args = tail_argv, ()
    if bazel_flags:
      logging.warning(
          'No "--" separator found; passing %s as Bazel flags. To pass flags'
          ' to %s, use: xmanager launch %s -- %s',
          list(bazel_flags),
          target,
          target,
          ' '.join(bazel_flags),
      )

  result = subprocess.run(
      [
          xm_flags.BAZEL_COMMAND.value,
          'run',
          *bazel_flags,
          target,
          '--',
          f'--xm_launch_script={target}',
          *script_args,
      ],
      check=False,
  )
  sys.exit(result.returncode)


def _launch_command(argv):
  """Launches an experiment using XManager."""
  if len(argv) < 3:
    raise app.UsageError('Please specify a launch script or Bazel target.')
  launch_script = argv[2]
  if launch_script.startswith(('//', ':')):  # Bazel target giveaways.
    _launch_bazel_target(launch_script, argv[3:])
    return
  if not os.path.exists(launch_script):
    raise OSError(errno.ENOENT, f'File not found: {launch_script}')
  sys.path.insert(0, os.path.abspath(os.path.dirname(launch_script)))
  launch_module, _ = os.path.splitext(os.path.basename(launch_script))
  m = importlib.import_module(launch_module)
  sys.path.pop(0)
  # Strip leading '--' so script flags are parsed by app.run(m.main).
  script_args = argv[3:]
  if script_args and script_args[0] == '--':
    script_args = script_args[1:]
  argv = [
      launch_script,
      '--xm_launch_script={}'.format(launch_script),
  ] + script_args
  app.run(m.main, argv=argv)


def _cluster_command(argv):
  """Creates or deletes a GKE cluster for use with xm_local."""
  if len(argv) < 3:
    raise app.UsageError(
        'Please specify a cluster create or delete subcommand.'
    )
  caliban_gke = importlib.import_module('caliban.platform.gke.cli')
  caliban_gke_types = importlib.import_module('caliban.platform.gke.types')
  subcmd = argv[2]
  args = {
      'dry_run': False,
      'cluster_name': _DEFAULT_CLUSTER_NAME,
      'zone': _DEFAULT_ZONE,
      'release_channel': caliban_gke_types.ReleaseChannel.REGULAR,
      'single_zone': True,
  }
  if subcmd == 'create':
    caliban_gke._cluster_create(args)  # pylint: disable=protected-access
  elif subcmd == 'delete':
    caliban_gke._cluster_delete(args)  # pylint: disable=protected-access
  else:
    raise app.UsageError(
        f'Subcommand `cluster {subcmd}` is not a supported subcommand'
    )


def _parse_flags(argv: Sequence[str]) -> list[str]:
  """Returns parsed XManager CLI flags, leaving `--` and script flags intact.

  Passing `known_only=True` tells Abseil to consume recognized XManager flags
  (like `--xm_bazel_command`), leave unknown flags (i.e. Bazel flags or script
  flags) untouched in `argv`, and preserve the `--` separator token.

  Args:
    argv: The raw command-line arguments passed to the XManager CLI.
  """
  return flags.FLAGS(argv, known_only=True)


def main(argv):
  if len(argv) < 2:
    raise app.UsageError(
        'Please specify a command. See `xmc help` for more details.'
    )
  match argv[1]:
    case 'help':
      _help_command(argv)
    case 'launch':
      _launch_command(argv)
    case 'cluster':
      _cluster_command(argv)
    case _:
      raise app.UsageError(f'Command `{argv[1]}` is not a supported command')


def entrypoint():
  app.run(main, flags_parser=_parse_flags)


if __name__ == '__main__':
  app.run(main, flags_parser=_parse_flags)
