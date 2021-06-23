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
"""A module for communicating with the Bazel server."""

import os
import subprocess
from typing import List, Sequence

from absl import flags
from xmanager.bazel import file_utils

from google.protobuf.internal.decoder import _DecodeVarint32
from xmanager.generated import build_event_stream_pb2 as bes_pb2

FLAGS = flags.FLAGS

flags.DEFINE_string('bazel_command', 'bazel', 'A command that runs Bazel.')


def _get_important_output(events: Sequence[bes_pb2.BuildEvent],
                          label: str) -> Sequence[bes_pb2.File]:
  for event in events:
    if event.id.HasField('target_completed'):
      # Note that we ignore `event.id.target_completed.aspect`.
      if event.id.target_completed.label == label:
        return event.completed.important_output
  raise ValueError(f'Missing target completion event for {label} in Bazel logs')


def _get_normalized_label(events: Sequence[bes_pb2.BuildEvent],
                          label: str) -> str:
  for event in events:
    if event.id.HasField('pattern'):
      assert len(event.id.pattern.pattern) == 1
      if event.id.pattern.pattern[0] == label:
        # Assume there is just one `TargetConfiguredId` child in such events.
        # Note that we ignore `event.children[0].target_configured.aspect`.
        return event.children[0].target_configured.label
  raise ValueError(f'Missing pattern expansion event for {label} in Bazel logs')


def _get_workspace_directory(events: Sequence[bes_pb2.BuildEvent]) -> str:
  for event in events:
    if event.id.HasField('started'):
      return event.started.workspace_directory
  raise ValueError('Missing start event in Bazel logs')


def _read_build_events(path: str) -> List[bes_pb2.BuildEvent]:
  """Parses build events from a file referenced by a given `path`.

  The file should contain serialized length-delimited`bes_pb2.BuildEvent`
  messages. See
  https://docs.bazel.build/versions/master/build-event-protocol.html#consume-in-binary-format
  for details.

  Args:
    path: Path to a file with the protocol.

  Returns:
    A list of build events.
  """
  with open(path, 'rb') as bep_file:
    buffer = bep_file.read()
    events = []
    position = 0
    while position < len(buffer):
      # Reimplementation of Java's `AbstractParser.parseDelimitedFrom` for
      # protobufs, which is not available in Python.
      size, start = _DecodeVarint32(buffer, position)
      event = bes_pb2.BuildEvent()
      event.ParseFromString(buffer[start:start + size])
      events.append(event)
      position = start + size
    return events


def _root_absolute_path() -> str:
  # If the launch script is run with Bazel, use `BUILD_WORKSPACE_DIRECTORY` to
  # get the root of the workspace where the build was initiated. If the launch
  # script is run with the CLI, query Bazel to find out.
  return os.getenv('BUILD_WORKSPACE_DIRECTORY') or subprocess.run(
      [FLAGS.bazel_command, 'info', 'workspace'],
      check=True,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
      universal_newlines=True,
  ).stdout.strip()


def build_single_target(label: str, tail_args: Sequence[str] = ()) -> List[str]:
  """Builds a target and returns paths to its important output.

  The definition of 'important artifacts in an output group' can be found at
  https://github.com/bazelbuild/bazel/blob/8346ea4cfdd9fbd170d51a528fee26f912dad2d5/src/main/java/com/google/devtools/build/lib/analysis/TopLevelArtifactHelper.java#L223-L224.

  Args:
    label: Label of the target to build.
    tail_args: Arguments to append to the Bazel command.

  Returns:
    A list of paths to the output.
  """
  with file_utils.TemporaryFilePath() as bep_path:
    subprocess.run(
        [
            FLAGS.bazel_command,
            'build',
            f'--build_event_binary_file={bep_path}',
            # Otherwise we may be reading an incomplete file below.
            '--bep_publish_used_heap_size_post_build',
            label,
        ] + list(tail_args),
        check=True,
        cwd=_root_absolute_path(),
    )
    events = _read_build_events(bep_path)
    normalized_label = _get_normalized_label(events, label)
    files = _get_important_output(events, normalized_label)
    workspace = _get_workspace_directory(events)
    return [
        os.path.join(workspace, *file.path_prefix, file.name) for file in files
    ]
