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
"""Module for transforming source code using copybara.

XManager primarily uses Copybara to run folder-to-folder workflows in the form:

core.workflow(
    name = "folder_to_folder",
    origin = folder.origin(),
    destination = folder.destination(),
    ...
)

Copybara allows for iterative local development on multiple platforms without
needing to add/remove platform-specific code modifications. This allows you to
preprocess source code so that it can be run on different platforms with
different executors. e.g.

local_version = run_workflow(config, 'local', path)
vertex_version = run_workflow(config, 'vertex', path)

local_spec = xm.PythonContainer(path=local_version, **kwargs)
vertex_spec = xm.PythonContainer(path=vertex_version, **kwargs)

[local_executable, vertex_executable] = experiment.package([
  xm.Packageable(
    executable_spec=spec,
    executor_spec=xm_local.Local.Spec()),
  xm.Packageable(
    executable_spec=spec,
    executor_spec=xm_local.Vertex.Spec())])

Copybara has no release process, so you must compile copybara yourself:
https://github.com/google/copybara
"""
import os
import subprocess
import tempfile
from typing import Optional

# Set with the compiled path to copybara e.g.
# COPYBARA_BIN = 'bazel-bin/java/com/google/copybara/copybara_deploy.jar'
COPYBARA_BIN = 'copybara'


def run_workflow(
    config: str,
    workflow: str,
    origin_folder: str,
    destination_folder: Optional[str] = None,
    config_root: Optional[str] = None,
) -> str:
  """Run a workflow in a copybara config to transform origin to destination.

  Args:
    config: Path to the Copybara config.
    workflow: Name of a workflow in copybara config.
    origin_folder: The origin folder to use as input. This will passed to
      Copybara via the source_ref argument.
    destination_folder: The destination folder to output.
    config_root: Configuration root path to be used for resolving absolute
      config labels like '//foo/bar'.

  Returns:
    The output destination folder.
  """
  origin_folder = os.path.abspath(origin_folder)
  if not destination_folder:
    destination_folder = tempfile.mkdtemp()
  command = [
      COPYBARA_BIN,
      config,
      workflow,
      '--ignore-noop',
      origin_folder,
      '--folder-dir=' + destination_folder,
  ]
  if config_root:
    command += ['--config-root=' + config_root]
  print('Copybara command: ', command)
  subprocess.run(command, check=True)
  return destination_folder
