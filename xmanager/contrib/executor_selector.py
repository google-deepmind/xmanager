# Copyright 2022 DeepMind Technologies Limited
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
"""Helper functions to create universal launchers.

This module provides an `--xm_launch_mode` flag to specify a desired executor
and two helper functions which return specific implementations of xm.Executor
and `create_experiment()`.

Usage:

Example 1 (simple executor tuning):

```
from xmanager.contrib import executor_selector
...

requirements = xm.JobRequirements(...)
executor = executor_selector.get_executor()(requirements=requirements)
...
with executor_selector.create_experiment(experiment_title='Run') as experiment:
  ...
```

Example 2 (involved executor tuning):
```
from xmanager.contrib import executor_selector
...

executor_fn = executor_selector.get_executor()
kwargs = {}
if isinstance(executor, xm_local.Vertex):
  # Fill in Vertex executor args
else:
  # Fill in Local executor args
```
executor = executor_fn(**kwargs)
"""

import enum
from typing import Callable, List, Optional, Union

from absl import flags
from xmanager import xm
from xmanager import xm_local


class XMLaunchMode(enum.Enum):
  """Specifies an executor to run an experiment."""
  VERTEX = 'vertex'
  LOCAL = 'local'
  INTERACTIVE = 'interactive'


_XM_LAUNCH_MODE = flags.DEFINE_enum_class(
    'xm_launch_mode',
    XMLaunchMode.VERTEX,
    XMLaunchMode,
    'How to launch the experiment. Supports local and interactive execution, ' +
    'launch on ' +
    'Vertex.')


def launch_mode() -> XMLaunchMode:
  return _XM_LAUNCH_MODE.value


def create_experiment(
    experiment_title: Optional[str] = None,
    mode: Optional[XMLaunchMode] = None) -> xm.Experiment:
  """Creates an experiment depending on the launch mode.

  Args:
    experiment_title: Title of an experiment
    mode: Specifies which experiment to create. If None, the '--xm_launch_mode'
      flag value is used.

  Returns:
    A newly created experiment.

  Raises:
    ValueError: if provided `mode` is unknown.
  """
  if mode is None:
    mode = launch_mode()

  if mode in (XMLaunchMode.LOCAL, XMLaunchMode.INTERACTIVE,
              XMLaunchMode.VERTEX):
    # TODO: add import here?
    return xm_local.create_experiment(experiment_title)
  raise ValueError(f'Unknown launch mode: {mode}')


def _local_executor(interactive: bool) -> Callable[..., xm.Executor]:
  """Helper to provide a local executor with appropriate `interactive` flag."""

  def setup_local(*args, **kwargs_in) -> xm_local.Local:
    kwargs = {}
    # Copy supported arguments.
    if 'experimental_stream_output' in kwargs_in:
      kwargs['experimental_stream_output'] = kwargs_in[
          'experimental_stream_output']
    # Set the specified value of `interactive`.
    docker_options = kwargs_in.get('docker_options', xm_local.DockerOptions())
    setattr(docker_options, 'interactive', interactive)
    kwargs['docker_options'] = docker_options
    return xm_local.Local(*args, **kwargs)

  return setup_local


def get_executor(
    mode: Optional[XMLaunchMode] = None) -> Callable[..., xm.Executor]:
  """Select an `xm.Executor` specialization depending on the launch mode.

  Args:
    mode: Specifies which class to select. If None, the '--xm_launch_mode' flag
      value is used.

  Returns:
    An executor constructor.

  Raises:
    ValueError: if provided `mode` is unknown.
  """
  if mode is None:
    mode = launch_mode()

  if mode == XMLaunchMode.VERTEX:
    return xm_local.Caip
  if (mode == XMLaunchMode.LOCAL or mode == XMLaunchMode.INTERACTIVE):
    return _local_executor(mode == XMLaunchMode.INTERACTIVE)
  raise ValueError(f'Unknown launch mode: {mode}')
