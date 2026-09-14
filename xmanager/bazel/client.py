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
from typing import Generic, Sequence, TypeVar

import attr
from xmanager.xm import resources as xm_resources

T = TypeVar('T')


# TODO: - Split this into a `BatchBuildResult` (per-target lists,
# returned by `build_targets.build_blaze_targets`) and a per-target
# `BuildResult` (scalars, produced by `router.py`), so the union types below and
# the `isinstance` narrowing in `router._populate_batch_results` can go away.
@attr.s(auto_attribs=True, frozen=True)
class BuildResult(Generic[T]):
  """Holds the result of a build operation.

  Attributes:
    resources: The resources produced by the build.
    build_id: The unique identifier for the build invocation (e.g., Sponge ID).
    skycache_outcome: Whether Skycache was enabled for the build, and if not,
      why. Corresponds to the `SkycacheDetails.Outcome` enum.
    portable_manifest_paths: Per-architecture manifest paths for multiarch
      support. At runtime this is:
      - `list[dict[xm.Architecture, str]]` (one per target) when returned from a
        batched build in `build_targets.py`.
      - `dict[xm.Architecture, str]` after being unpacked per-target in
        `router.py`.
      - `None` for non-multiarch builds or when manifest paths are absent.
    flat_pkgdef_path: Path to the generated .flat_pkgdef file from
      TemporalMpmAspect, used for binary size calculation in ObjFS deployment
      and MPM packaging. At runtime this is:
      - `list[str | None]` (one per target) when returned from a batched build
        in `build_targets.py`.
      - `str | None` after being unpacked per-target in `router.py`.
      - `None` when absent or aspect output is unavailable.
  """  # fmt: skip

  resources: T
  build_id: str | None = None
  skycache_outcome: int = 0
  portable_manifest_paths: (
      list[dict[xm_resources.Architecture, str]]
      | dict[xm_resources.Architecture, str]
      | None
  ) = None
  flat_pkgdef_path: list[str | None] | str | None = None


class BazelService(abc.ABC):
  """An interface for Bazel operations."""

  @abc.abstractmethod
  def build_targets(
      self, labels: Sequence[str], tail_args: Sequence[str]
  ) -> BuildResult[list[list[str]]]:
    """Builds given targets and returns paths to their important outputs.

    Args:
      labels: Labels of the targets to build.
      tail_args: Arguments to append to the Bazel command.

    Returns:
      A BuildResult object, where `BuildResult.resources[i]` is a list of
      important outputs for `labels[i]`, and `BuildResult.build_id` is the build
      identifier for the build invocation.
    """
    raise NotImplementedError


def _to_tuple(sequence: Sequence[str]) -> tuple[str, ...]:
  """A standalone function to satisfy PyType."""
  return tuple(sequence)


@attr.s(auto_attribs=True, frozen=True)
class BazelTarget:
  """A Bazel target to be built."""

  label: str
  bazel_args: tuple[str, ...] = attr.ib(converter=_to_tuple)
