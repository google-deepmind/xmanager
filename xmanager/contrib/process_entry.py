# Copyright 2020 DeepMind Technologies Limited. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Entry point of a user-defined Python function."""


import contextlib
import functools
import os
import sys

from absl import app
from absl import flags
from absl import logging
import cloudpickle


FLAGS = flags.FLAGS
flags.DEFINE_string(
    'data_file', '', 'Pickle file location with entry points for all nodes'
)
flags.DEFINE_string(
    'init_file',
    '',
    'Pickle file location containing initialization module '
    'executed for each node prior to an entry point',
)
flags.DEFINE_string('flags_to_populate', '{}', 'obsolete')


def _parse_process_entry_flags(all_argv: list[str]) -> list[str]:
  """Parse and consume all flags for the entry script; return the rest."""
  # unconsumed_argv will still include all_argv[0], which is expected to be
  # the program name and is ignored by flag parsing.
  unconsumed_argv = FLAGS(all_argv, known_only=True)

  # JAX doesn't use absl flags and so we need to forward absl flags to JAX
  # explicitly. Here's a heuristic to detect JAX flags and forward them.
  if any(arg.startswith('--jax_') for arg in sys.argv):
    try:
      # pytype:disable=import-error
      # pylint:disable=g-import-not-at-top
      import jax
      # pytype:enable=import-error
      # pylint:enable=g-import-not-at-top
      jax.config.parse_flags_with_absl()
    except ImportError:
      pass

  return unconsumed_argv


def main(argv: list[str], process_argv: list[str]):
  # See `parse_flags_and_run()` for why arguments are passed in `process_argv`
  # instead.
  assert len(argv) == 1
  del argv

  # Allow for importing modules from the current directory.
  sys.path.append(os.getcwd())
  data_file = FLAGS.data_file
  init_file = FLAGS.init_file

  if os.environ.get('TF_CONFIG', None):
    # For GCP runtime log to STDOUT so that logs are not reported as errors.
    logging.get_absl_handler().python_handler.stream = sys.stdout

  if init_file:
    init_function = cloudpickle.load(open(init_file, 'rb'))
    init_function()
  functions = cloudpickle.load(open(data_file, 'rb'))

  # Now that the code that we intend to run has been unpickled, that should
  # have caused the registration of any remaining flags that the program needs.
  [unused_program_name, *unconsumed_argv] = FLAGS(process_argv, known_only=True)
  if unconsumed_argv:
    logging.warning(
        'The following command-line arguments were passed to the '
        'program but are not used by anything that it imports: %s',
        unconsumed_argv,
    )

  with contextlib.suppress():  # no-op context manager
    # Currently only one function is supported.
    functions[0]()


def parse_flags_and_run():
  # Parse flags for this module and the things it has already imported.
  # Pass whatever flags are left over to main() through a side channel, so that
  # app.run() doesn't try to parse them before we have set the scene.
  [program_name, *process_argv] = _parse_process_entry_flags(sys.argv)
  app.run(
      functools.partial(main, process_argv=[program_name, *process_argv]),
      argv=[program_name],
  )


if __name__ == '__main__':
  parse_flags_and_run()
