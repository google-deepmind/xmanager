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
"""Packaging for execution on Cloud."""
from typing import Optional

from xmanager import xm
from xmanager.bazel import client as bazel_client
from xmanager.cloud import auth
from xmanager.cloud import build_image
from xmanager.cloud import docker_lib
from xmanager.docker import docker_adapter
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import executors as local_executors
from xmanager.xm_local.packaging import bazel_tools


def _get_push_image_tag(executor_spec: xm.ExecutorSpec) -> Optional[str]:
  """Get the push_image_tag from executor or None."""
  match executor_spec:
    case local_executors.CaipSpec() as caip_spec:
      return caip_spec.push_image_tag
    case local_executors.KubernetesSpec() as kubernetes_spec:
      return kubernetes_spec.push_image_tag
    case _:
      raise TypeError(
          f'Unsupported executor specification: {executor_spec!r}. '
      )


def _package_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    container: xm.Container,
) -> xm.Executable:
  """Matcher method for packaging `xm.Container`.

  If the user builds an image with the same repository name as an image in a
  remote Cloud registry, the user should push the image before running XManager,
  because XManager will not push a local image in the packaging step.

  Unless the container's image path already matches the GCR project prefix, we
  always pull the container and push it to push_image_tag location. This avoids
  a potential permissions error where the user has permissions to read an image,
  but the Cloud service agent does not have permissions. If container.image_path
  already points to the GCR project, we skip pushing because the image should
  already be in the destination location.

  Args:
    bazel_outputs: TargetOutputs mapping from Bazel targets to outputs.
    packageable: Packageable containing Executor and Executable.
    container: Container specifying image path.

  Returns:
    GoogleContainerRegistryImage Executable.
  """
  del bazel_outputs
  gcr_project_prefix = 'gcr.io/' + auth.get_project_name()
  if (
      container.image_path.startswith(gcr_project_prefix)
      or not docker_lib.is_docker_installed()
  ):
    return local_executables.GoogleContainerRegistryImage(
        name=packageable.executable_spec.name,
        image_path=container.image_path,
        args=packageable.args,
        env_vars=packageable.env_vars,
    )

  instance = docker_adapter.instance()
  client = instance.get_client()
  print(f'Pulling {container.image_path}...')
  repository, tag = instance.split_tag(container.image_path)
  image_id = instance.pull_image(container.image_path)
  image = client.images.get(image_id)

  push_image_tag = _get_push_image_tag(packageable.executor_spec)
  if not push_image_tag:
    if repository.startswith(gcr_project_prefix):
      # If the image path already points to the project's GCR, reuse it.
      push_image_tag = f'{repository}:{tag}'
    else:
      # Otherwise, create a new image repository inside the project's GCR.
      push_image_tag = f'{gcr_project_prefix}/{repository}:{tag}'
  image.tag(push_image_tag)
  print(f'Pushing {push_image_tag}...')
  client.images.push(push_image_tag)
  return local_executables.GoogleContainerRegistryImage(
      name=packageable.executable_spec.name,
      image_path=push_image_tag,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_dockerfile(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    dockerfile: xm.Dockerfile,
):
  """Matcher method for packaging `xm.Dockerfile`."""
  del bazel_outputs
  push_image_tag = _get_push_image_tag(packageable.executor_spec)
  if not push_image_tag:
    gcr_project_prefix = 'gcr.io/' + auth.get_project_name()
    tag = docker_lib.create_tag()
    push_image_tag = f'{gcr_project_prefix}/{dockerfile.name}:{tag}'

  image = build_image.build_by_dockerfile(
      dockerfile.path,
      dockerfile.dockerfile,
      push_image_tag,
      pull_image=docker_lib.is_docker_installed(),
  )
  if docker_lib.is_docker_installed():
    build_image.push(image)
  return local_executables.GoogleContainerRegistryImage(
      name=packageable.executable_spec.name,
      image_path=push_image_tag,
      args=packageable.args,
      env_vars=packageable.env_vars,
  )


def _package_python_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    python_container: xm.PythonContainer,
) -> xm.Executable:
  """Matcher method for packaging `xm.PythonContainer`."""
  del bazel_outputs
  push_image_tag = _get_push_image_tag(packageable.executor_spec)
  image = build_image.build(
      python_container,
      packageable.args,
      packageable.env_vars,
      push_image_tag,
      pull_image=docker_lib.is_docker_installed(),
  )
  if docker_lib.is_docker_installed():
    build_image.push(image)
  return local_executables.GoogleContainerRegistryImage(
      name=packageable.executable_spec.name,
      image_path=image,
  )


def _package_bazel_container(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    bazel_container: xm.BazelContainer,
):
  """Matcher method for packaging `xm.BazelContainer`."""
  paths = bazel_outputs[
      bazel_client.BazelTarget(
          label=bazel_container.label,
          bazel_args=bazel_container.bazel_args,
      )
  ]

  instance = docker_adapter.instance()
  client = instance.get_client()

  push_image_tag = _get_push_image_tag(packageable.executor_spec)
  print(f'Loading {bazel_container.label}...')
  loaded_image_id = instance.load_image(paths[0])

  image = client.images.get(loaded_image_id)
  image.tag(push_image_tag)
  print(f'Pushing {push_image_tag}...')
  client.images.push(push_image_tag)

  return local_executables.GoogleContainerRegistryImage(
      name=packageable.executable_spec.name,
      image_path=push_image_tag,
  )


def package_cloud_executable(
    bazel_outputs: bazel_tools.TargetOutputs,
    packageable: xm.Packageable,
    executable_spec: xm.ExecutableSpec,
) -> xm.Executable:
  match executable_spec:
    case xm.Container() as container:
      return _package_container(
          bazel_outputs,
          packageable,
          container,
      )
    case xm.Dockerfile() as dockerfile:
      return _package_dockerfile(
          bazel_outputs,
          packageable,
          dockerfile,
      )
    case xm.PythonContainer() as python_container:
      return _package_python_container(
          bazel_outputs,
          packageable,
          python_container,
      )
    case xm.BazelContainer() as bazel_container:
      return _package_bazel_container(
          bazel_outputs,
          packageable,
          bazel_container,
      )
    case _:
      raise TypeError(
          'Unsupported executable specification '
          f'for Cloud packaging: {executable_spec!r}'
      )
