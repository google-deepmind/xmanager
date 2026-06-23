"""Methods for routing packageables to the appropriate packager."""

import collections
import datetime
import os
from typing import Sequence

from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.xm_cloud import executor
from xmanager.xm_local import executables
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local.packaging import bazel_tools
from xmanager.xm_local.packaging import cloud as cloud_packaging


def _get_push_image_tag(packageable: xm.Packageable) -> str | None:
  """Gets the push_image_tag from the packageable's executor spec."""
  if not isinstance(
      packageable.executor_spec, executor.KubernetesJobExecutorSpec
  ):
    return None
  executor_spec = packageable.executor_spec
  if not executor_spec.container_registry:
    return None
  image_name = executor_spec.image_name or packageable.executable_spec.name
  default_image_tag = (
      f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-'
      f'{os.environ.get("USER")}'
  )
  image_tag = executor_spec.image_tag or default_image_tag

  return f'{executor_spec.container_registry}/{image_name}:{image_tag}'


def _convert_to_local_executor_spec(
    packageable: xm.Packageable,
) -> xm.Packageable:
  """Convert a packageable with a KubernetesJobExecutorSpec to KubernetesSpec."""
  # If the executor spec is not a KubernetesJobExecutorSpec, return the
  # packageable as-is. If there is an unsupported executor spec type, the
  # packaging router will raise an error, but there is no need to check for it
  # again here.
  if not isinstance(
      packageable.executor_spec, executor.KubernetesJobExecutorSpec
  ):
    return packageable
  packageable.executor_spec = local_executors.KubernetesSpec(
      push_image_tag=_get_push_image_tag(packageable),
  )
  return packageable


def _packaging_router(
    built_targets: bazel_tools.TargetOutputs, packageable: xm.Packageable
) -> executables.GoogleContainerRegistryImage:
  """Routes a packageable to the appropriate packaging mechanism."""
  match packageable.executor_spec:
    case executor.KubernetesJobExecutorSpec():
      # Convert the KubernetesJobExecutorSpec to a KubernetesSpec so that it can
      # be used with the xm_local packaging code without adding a dependency on
      # xm_cloud there. This does not impact the actual executor for the job.
      packageable = _convert_to_local_executor_spec(packageable)
    case _:
      raise TypeError(
          f'Unsupported executor specification: {packageable.executor_spec!r}. '
          f'Packageable: {packageable!r}'
      )

  match packageable.executable_spec:
    case (
        xm.Container()
        | xm.Dockerfile()
        | xm.BazelContainer()
        | xm.PythonContainer()
    ):
      return cloud_packaging.package_cloud_executable(
          built_targets,
          packageable,
          packageable.executable_spec,
      )
    case _:
      # TODO: - fix support for other packageable types.
      raise TypeError(
          'Unsupported packageable type for KubernetesJobExecutorSpec: '
          f'{packageable.executable_spec!r}'
      )


_ArgsToTargets = dict[tuple[str, ...], list[bazel_client.BazelTarget]]


def package(packageables: Sequence[xm.Packageable]) -> Sequence[xm.Executable]:
  """Routes a packageable to an appropriate packaging mechanism."""
  built_targets: bazel_tools.TargetOutputs = {}
  bazel_targets = bazel_tools.collect_bazel_targets(packageables)

  if bazel_targets:
    bazel_service = bazel_tools.local_bazel_service()

    args_to_targets: _ArgsToTargets = collections.defaultdict(list)
    for target in bazel_targets:
      args_to_targets[target.bazel_args].append(target)

    for args, targets in args_to_targets.items():
      outputs = bazel_service.build_targets(
          labels=tuple(target.label for target in targets),
          bazel_args=args,
      )
      for target, output in zip(targets, outputs.resources):
        built_targets[target] = output

  return [
      _packaging_router(built_targets, packageable)
      for packageable in packageables
  ]
