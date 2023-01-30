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

import abc
from typing import List, Sequence, Tuple

import attr


class BazelService(abc.ABC):
  """An interface for Bazel operations."""

  @abc.abstractmethod
  def fetch_kinds(self, labels: Sequence[str]) -> List[str]:
    """Fetches kinds of given targets.

    See https://docs.bazel.build/versions/main/query.html#output-label_kind.

    Args:
      labels: Labels of the targets to query.

    Returns:
      A list of kinds, for example, `['py_binary rule']`.
    """
    raise NotImplementedError

  @abc.abstractmethod
  def build_targets(
      self, labels: Sequence[str], tail_args: Sequence[str]
  ) -> List[List[str]]:
    """Builds given targets and returns paths to their important outputs.

    Args:
      labels: Labels of the targets to build.
      tail_args: Arguments to append to the Bazel command.

    Returns:
      For each label returns a list of its important outputs.
    """
    raise NotImplementedError


def _to_tuple(sequence: Sequence[str]) -> Tuple[str, ...]:
  """A standalone function to satisfy PyType."""
  return tuple(sequence)


@attr.s(auto_attribs=True, frozen=True)
class BazelTarget:
  """A Bazel target to be built."""

  label: str
  bazel_args: Tuple[str, ...] = attr.ib(converter=_to_tuple)
