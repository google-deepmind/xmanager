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
"""Bazel tools for local packaging."""

import collections
import functools
import itertools
import os
import re
import subprocess
from typing import Dict, List, Optional, Sequence, Tuple

from xmanager import xm
from xmanager import xm_flags
from xmanager.bazel import client
from xmanager.bazel import file_utils

from google.protobuf.internal.decoder import _DecodeVarint32
from xmanager.generated import build_event_stream_pb2 as bes_pb2


def _get_important_outputs(
    events: Sequence[bes_pb2.BuildEvent], labels: Sequence[str]
) -> List[List[bes_pb2.File]]:
  named_sets = {}
  for event in events:
    if event.id.HasField('named_set'):
      named_sets[event.id.named_set.id] = event.named_set_of_files

  # TODO(hartikainen): Is this the right way to get the binary directory? See usage
  # below.
  bindir = (
      next(e for e in events if e.id.HasField("configuration"))
      .configuration
      .make_variable['BINDIR']
  )

  label_to_output =  collections.defaultdict(list)
  for event in events:
    if event.id.HasField('target_completed'):
      label = event.id.target_completed.label

      # Start with whatever is in important_output (may be empty).
      outputs = list(event.completed.important_output)

      # Collect from output_group.file_sets references
      for group in event.completed.output_group:
        for file_set in group.file_sets:
          queue = collections.deque([file_set.id])
          visited = set()
          while queue:
            current = queue.popleft()
            if current in visited:
              continue
            visited.add(current)
            named_set = named_sets.get(current)
            if named_set:
              # TODO(hartikainen): Check this logic.
              # NOTE(hartikainen): Only gather binary outputs. `py_binary` targets, for
              # example, include more than one file in the output group (one for the
              # binary and at least one for the source code), but I think we only care
              # for the binary.
              binary_outputs = [file for file in named_set.files if bindir in file.uri]
              outputs.extend(binary_outputs)
              for nested in named_set.file_sets:
                queue.append(nested.id)

      label_to_output[label] = outputs

  return [label_to_output[label] for label in labels]


def _get_normalized_labels(
    events: Sequence[bes_pb2.BuildEvent], labels: Sequence[str]
) -> List[str]:
  label_to_expansion: Dict[str, str] = {}
  for event in events:
    if event.id.HasField('pattern'):
      for index, pattern in enumerate(event.id.pattern.pattern):
        # Note that we ignore `event.children.target_configured.aspect`.
        label_to_expansion[pattern] = event.children[
            index
        ].target_configured.label
  return [label_to_expansion[label] for label in labels]


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
      event.ParseFromString(buffer[start : start + size])
      events.append(event)
      position = start + size
    return events


def _root_absolute_path() -> str:
  # If the launch script is run with Bazel, use `BUILD_WORKSPACE_DIRECTORY` to
  # get the root of the workspace where the build was initiated. If the launch
  # script is run with the CLI, query Bazel to find out.
  return (
      os.getenv('BUILD_WORKSPACE_DIRECTORY')
      or subprocess.run(
          [xm_flags.BAZEL_COMMAND.value, 'info', 'workspace'],
          check=True,
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE,
          universal_newlines=True,
      ).stdout.strip()
  )


def _build_multiple_targets(
    labels: Sequence[str], bazel_args: Sequence[str] = ()
) -> List[List[str]]:
  """Builds the targets and returns paths to their important outputs.

  The definition of 'important artifacts in an output group' can be found at
  https://github.com/bazelbuild/bazel/blob/8346ea4cfdd9fbd170d51a528fee26f912dad2d5/src/main/java/com/google/devtools/build/lib/analysis/TopLevelArtifactHelper.java#L223-L224.

  Args:
    labels: Labels of the targets to build.
    bazel_args: Arguments to append to the Bazel command.

  Returns:
    A list of paths to the output.
  """
  with file_utils.TemporaryFilePath() as bep_path:
    subprocess.run(
        [
            xm_flags.BAZEL_COMMAND.value,
            'build',
            f'--build_event_binary_file={bep_path}',
            # Forces a GC at the end of the build and publishes value to BEP.
            '--memory_profile=/dev/null',
            *labels,
            *bazel_args,
        ],
        check=True,
        cwd=_root_absolute_path(),
    )
    events = _read_build_events(bep_path)
    normalized_labels = _get_normalized_labels(events, labels)
    output_lists = _get_important_outputs(events, normalized_labels)
    workspace = _get_workspace_directory(events)
    results: List[List[str]] = []
    for files in output_lists:
      results.append(
          [
              os.path.join(workspace, *file.path_prefix, file.name)
              for file in files
          ]
      )
    return results


# Expansions (`...`, `*`) are not allowed.
_NAME_RE = r'(?:[^.*:/]|\.(?!\.\.))+'
_LABEL_LEXER = re.compile(
    f'^//(?P<packages>{_NAME_RE}(/{_NAME_RE})*)?(?P<target>:{_NAME_RE})?$'
)
_LexedLabel = Tuple[List[str], str]


def _lex_label(label: str) -> _LexedLabel:
  """Splits the label into packages and target."""
  match = _LABEL_LEXER.match(label)
  if match is None:
    raise ValueError(f'{label} is not an absolute Bazel label')
  groups = match.groupdict()
  packages: Optional[str] = groups['packages']
  target: Optional[str] = groups['target']
  if not packages and not target:
    raise ValueError(f'{label} cannot be empty')
  if target == ':all':
    raise ValueError('`:all` is not a valid target')
  init = packages.split('/') if packages else []
  last = target[1:] if target else init[-1]
  return init, last


def _assemble_label(parts: _LexedLabel) -> str:
  init, last = parts
  return f"//{'/'.join(init)}:{last}"


def _label_kind_lines_to_dict(lines: Sequence[str]) -> Dict[str, str]:
  kind_label_tuples = [line.rsplit(' ', 1) for line in lines]
  return {label: kind for kind, label in kind_label_tuples}


class LocalBazelService(client.BazelService):
  """Local implementation of `BazelService`."""

  def fetch_kinds(self, labels: Sequence[str]) -> List[str]:
    """Retrieves kind for each given target in the current workspace."""
    labels = [_assemble_label(_lex_label(label)) for label in labels]

    # For each matching target `bazel query` produces a line formatted as
    # `<rule name> rule <target name>`, for example, `py_library rule
    # //third_party/py/xmanager/xm:__init__`. See
    # https://docs.bazel.build/versions/main/query.html#output-label_kind.
    stdout = subprocess.run(
        [
            xm_flags.BAZEL_COMMAND.value,
            'query',
            f"'{' union '.join(labels)}'",
            '--output',
            'label_kind',
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=_root_absolute_path(),
    ).stdout.decode('utf-8')
    label_kinds = _label_kind_lines_to_dict(stdout.strip().split(os.linesep))
    return [label_kinds[label] for label in labels]

  def build_targets(
      self, labels: Sequence[str], bazel_args: Sequence[str]
  ) -> List[List[str]]:
    return _build_multiple_targets(labels, bazel_args)


@functools.lru_cache()
def local_bazel_service() -> LocalBazelService:
  """Returns a singleton instance of `LocalBazelService`."""
  return LocalBazelService()


def apply_default_bazel_args(args: list[str]) -> tuple[str, ...]:
  """Returns bazel flags to be used with additional args applied."""
  return args


def _collect_executables(
    executable: xm.ExecutableSpec,
) -> List[client.BazelTarget]:
  match executable:
    case xm.BazelBinary() as bazel_binary:
      return [
          client.BazelTarget(
              label=bazel_binary.label,
              bazel_args=apply_default_bazel_args(bazel_binary.bazel_args),
          ),
      ]
    case xm.BazelContainer() as bazel_container:
      return [
          client.BazelTarget(
              label=bazel_container.label,
              bazel_args=apply_default_bazel_args(bazel_container.bazel_args),
          ),
      ]
    case _:
      return []


def collect_bazel_targets(
    packageables: Sequence[xm.Packageable],
) -> List[client.BazelTarget]:
  """Extracts Bazel targets to package from a sequence of `Packageable`s."""
  return list(
      itertools.chain(
          *[
              _collect_executables(packageable.executable_spec)
              for packageable in packageables
          ]
      )
  )


TargetOutputs = Dict[client.BazelTarget, List[str]]
