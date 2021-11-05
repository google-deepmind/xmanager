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
"""Methods for routing packageables to appropriate packagers."""

import collections
from typing import Any, Dict, List, Sequence, Tuple

from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.xm import pattern_matching
from xmanager.xm_local import executors
from xmanager.xm_local.packaging import bazel_tools
from xmanager.xm_local.packaging import cloud as cloud_packaging
from xmanager.xm_local.packaging import local as local_packaging


def _visit_caip_spec(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    _: executors.CaipSpec,
):
  del bazel_outputs
  return cloud_packaging.package_cloud_executable(
      packageable,
      packageable.executable_spec,
  )


def _visit_local_spec(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    _: executors.LocalSpec,
):
  return local_packaging.package_for_local_executor(
      bazel_outputs,
      packageable,
      packageable.executable_spec,
  )


def _visit_kubernetes_spec(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    _: executors.KubernetesSpec,
):
  del bazel_outputs
  return cloud_packaging.package_cloud_executable(
      packageable,
      packageable.executable_spec,
  )


def _throw_on_unknown_executor(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    executor: Any,
):
  raise TypeError(f'Unsupported executor specification: {executor!r}. '
                  f'Packageable: {packageable!r}')


_PACKAGING_ROUTER = pattern_matching.match(
    _visit_caip_spec,
    _visit_local_spec,
    _visit_kubernetes_spec,
    _throw_on_unknown_executor,
)


def _normalize_label(label: str, kind: str) -> str:
  """Attempts to correct the label if it does not point to the right target.

  In certain cases people might specify labels that do not correspond to the
  desired output. For example, for a `py_binary(name='foo', ...)` target the
  self-contained executable is actually called 'foo.par'.

  Args:
    label: The target's name.
    kind: The target's kind.

  Returns:
    Either the same or a corrected label.
  """
  if kind == 'py_binary rule' and not label.endswith('.par'):
    return f'{label}.par'
  return label


_ArgsToTargets = Dict[Tuple[str, ...], List[bazel_client.BazelTarget]]


def package(packageables: Sequence[xm.Packageable]) -> List[xm.Executable]:
  """Routes a packageable to an appropriate packaging mechanism."""
  bazel_targets = bazel_tools.collect_bazel_targets(packageables)

  bazel_labels = [target.label for target in bazel_targets]
  bazel_kinds = bazel_tools.local_bazel_service().fetch_kinds(bazel_labels)
  label_to_kind = dict(zip(bazel_labels, bazel_kinds))

  args_to_targets: _ArgsToTargets = collections.defaultdict(list)
  for target in bazel_targets:
    args_to_targets[target.bazel_args].append(target)

  built_targets: bazel_tools.TargetOutputs = {}
  for args, targets in args_to_targets.items():
    outputs = bazel_tools.local_bazel_service().build_targets(
        labels=[
            _normalize_label(target.label, label_to_kind[target.label])
            for target in targets
        ],
        tail_args=args,
    )
    for target, output in zip(targets, outputs):
      built_targets[target] = output

  return [
      _PACKAGING_ROUTER(built_targets, packageable, packageable.executor_spec)
      for packageable in packageables
  ]
