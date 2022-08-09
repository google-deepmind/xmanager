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
"""Local backend executors."""

from typing import Dict, Optional

import attr
from xmanager import xm
from xmanager.docker import docker_adapter

GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER = 'GOOGLE_KUBERNETES_ENGINE'


@attr.s(auto_attribs=True)
class LocalSpec(xm.ExecutorSpec):
  """Current machine executor's specification."""


@attr.s(auto_attribs=True)
class DockerOptions:
  """Options of the container to be run.

  Attributes:
      ports: In the simplest form -- a dictionary from `int` to `int`, where the
        keys represent the ports inside the container and the values represent
        the ports of the host to bind. See the specification at
        https://docker-py.readthedocs.io/en/stable/containers.html.
      volumes: A dictionary from `str` to `str`, where the keys represent paths
        inside the host to mount and the values represent paths in the
        container.
      mount_gcs_path: If True, checks for the `~/gcs` directory on the host
        and mounts it (if found) at `/gcs` in the container. Defaults to True.
      interactive: If True, requests a run with interactive shell.
  """
  ports: Optional[docker_adapter.Ports] = None
  volumes: Optional[Dict[str, str]] = None
  mount_gcs_path: bool = True
  interactive: bool = False


@attr.s(auto_attribs=True)
class Local(xm.Executor):
  """Current machine executor.

  Attributes:
    docker_options: Options applied if the job is a container-based executable.
    experimental_stream_output: Whether to pipe the job's stdout and stderr to
      the terminal. Might be removed once we decide on the logging design.
  """

  docker_options: Optional[DockerOptions] = None
  experimental_stream_output: bool = True

  Spec = LocalSpec  # pylint: disable=invalid-name


@attr.s(auto_attribs=True)
class TpuCapability:
  """TPU capability configures the TPU software requested by an executor."""

  # Read about TPU versions:
  # https://cloud.google.com/tpu/docs/version-switching
  tpu_runtime_version: str


@attr.s(auto_attribs=True)
class TensorboardCapability:
  """Tensorboard capability integrates a Vertex AI Job with Tensorboard."""

  # The name of the tensorboard to use.
  name: str
  # The "gs://$GCS_BUCKET/dir_name" to save output.
  # Tensorboard will read the logs from $BASE_OUTPUT_DIRECTORY/logs/
  # If None, then the root of the default bucket will be used.
  base_output_directory: Optional[str] = None


@attr.s(auto_attribs=True)
class VertexSpec(xm.ExecutorSpec):
  """Vertex AI spec describes the Google Cloud Platform (GCP) location."""

  # An image registry name tag to push.
  # The image tag should be in the form 'myregistryhost/name:tag'
  push_image_tag: Optional[str] = None


@attr.s(auto_attribs=True)
class Vertex(xm.Executor):
  """Vertex AI Executor describes the runtime environment of GCP."""

  requirements: xm.JobRequirements = attr.Factory(xm.JobRequirements)
  tensorboard: Optional[TensorboardCapability] = None

  Spec = VertexSpec  # pylint: disable=invalid-name


# Declaring variable aliases for legacy compatability.
# New code should not use these aliases.
Caip = Vertex
CaipSpec = VertexSpec


@attr.s(auto_attribs=True)
class KubernetesSpec(xm.ExecutorSpec):
  """K8s spec describes the K8s location."""

  # An image registry name tag to push.
  # The image tag should be in the form 'myregistryhost/name:tag'
  push_image_tag: Optional[str] = None


@attr.s(auto_attribs=True)
class Kubernetes(xm.Executor):
  """K8s Executor describes the runtime environment of Kubernetes."""

  requirements: xm.JobRequirements = attr.Factory(xm.JobRequirements)
  cloud_provider: str = GOOGLE_KUBERNETES_ENGINE_CLOUD_PROVIDER
  tpu_capability: Optional[TpuCapability] = None

  Spec = KubernetesSpec  # pylint: disable=invalid-name
