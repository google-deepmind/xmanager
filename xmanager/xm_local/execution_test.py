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
"""Tests for xmanager.xm_local.executors."""

import os
import subprocess
import sys
import unittest
from unittest import mock

from absl import flags
from absl.testing import parameterized
import docker
from xmanager import xm
from xmanager.docker import docker_adapter
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import execution
from xmanager.xm_local import executors as local_executors


def create_test_job(interactive: bool = False,
                    mount_gcs_path: bool = True) -> xm.Job:
  return xm.Job(
      name='test-job',
      executable=local_executables.LoadedContainerImage(
          name='test',
          image_id='test-image',
          args=xm.SequentialArgs.from_collection({'a': 1}),
          env_vars={'c': '0'}),
      executor=local_executors.Local(
          docker_options=local_executors.DockerOptions(
              ports={8080: 8080},
              volumes={'a': 'b'},
              interactive=interactive,
              mount_gcs_path=mount_gcs_path)))


class ExecutionTest(unittest.IsolatedAsyncioTestCase, parameterized.TestCase):
  """Tests for xm_local.execution (currently only for container launches)."""

  async def asyncSetUp(self):
    # Force flag initialization to avoid errors
    flags.FLAGS(sys.argv)

  @parameterized.product(
      interactive=[True, False],
      mount_gcs_path=[True, False],
      gcs_dir_exists=[True, False])
  @mock.patch.object(
      docker_adapter.DockerAdapter, 'run_container', return_value=None)
  async def test_container_launch_dispatcher(self, mock_run_container,
                                             interactive, mount_gcs_path,
                                             gcs_dir_exists):
    """Tests if the container launch dispatcher is called correctly when using `xm_local.execution.launch`."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True

    job = create_test_job(
        interactive=interactive, mount_gcs_path=mount_gcs_path)

    mock_gcs_dir_existence = (
        lambda path: gcs_dir_exists and path.endswith('/gcs'))

    with mock.patch.object(docker, 'from_env',
                           return_value=mock_docker_client), \
         mock.patch.object(os.path, 'isdir',
                           side_effect=mock_gcs_dir_existence):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_call_kwargs = {
        'name': 'test-job',
        'image_id': 'test-image',
        'network': 'xmanager',
        'args': ['--a=1'],
        'env_vars': {
            'c': '0'
        },
        'ports': {
            8080: 8080
        },
        'volumes': {
            'a': 'b',
            os.path.expanduser('~/.config/gcloud'): '/root/.config/gcloud'
        },
        'interactive': interactive
    }

    if mount_gcs_path and gcs_dir_exists:
      expected_call_kwargs['volumes'][os.path.expanduser('~/gcs')] = '/gcs'

    mock_run_container.assert_called_once_with(**expected_call_kwargs)

  @parameterized.product(
      mount_gcs_path=[True, False], gcs_dir_exists=[True, False])
  @mock.patch.object(
      docker.models.containers.ContainerCollection, 'run', return_value=None)
  async def test_container_launch_by_client(self, mock_client_run,
                                            mount_gcs_path, gcs_dir_exists):
    """Tests if the Docker Python client launches containers correctly when using `xm_local.execution.launch`."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True
    mock_docker_client.containers = (
        docker.models.containers.ContainerCollection(None))

    job = create_test_job(interactive=False, mount_gcs_path=mount_gcs_path)
    mock_gcs_dir_existence = (
        lambda path: gcs_dir_exists and path.endswith('/gcs'))

    with mock.patch.object(docker, 'from_env',
                           return_value=mock_docker_client), \
         mock.patch.object(os.path, 'isdir',
                           side_effect=mock_gcs_dir_existence):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_call_kwargs = {
        'name': 'test-job',
        'hostname': 'test-job',
        'network': 'xmanager',
        'detach': True,
        'remove': True,
        'command': ['--a=1'],
        'environment': {
            'c': '0'
        },
        'ports': {
            8080: 8080
        },
        'volumes': {
            'a': {
                'bind': 'b',
                'mode': 'rw'
            },
            os.path.expanduser('~/.config/gcloud'): {
                'bind': '/root/.config/gcloud',
                'mode': 'rw'
            }
        }
    }
    if mount_gcs_path and gcs_dir_exists:
      expected_call_kwargs['volumes'][os.path.expanduser('~/gcs')] = {
          'bind': '/gcs',
          'mode': 'rw'
      }

    mock_client_run.assert_called_once_with('test-image',
                                            **expected_call_kwargs)

  @parameterized.product(
      mount_gcs_path=[True, False], gcs_dir_exists=[True, False])
  @mock.patch.object(subprocess, 'run', return_value=None)
  async def test_container_launch_by_subprocess(
      self, mock_container_launch_by_subprocess, mount_gcs_path,
      gcs_dir_exists):
    """Tests if the Docker subprocesses are created correctly when using `xm_local.execution.launch."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True

    job = create_test_job(interactive=True, mount_gcs_path=mount_gcs_path)
    mock_gcs_dir_existence = (
        lambda path: gcs_dir_exists and path.endswith('/gcs'))

    with mock.patch.object(docker, 'from_env',
                           return_value=mock_docker_client), \
         mock.patch.object(os.path, 'isdir',
                           side_effect=mock_gcs_dir_existence):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_gcs_path_args = []
    if mount_gcs_path and gcs_dir_exists:
      expected_gcs_path_args = ['-v', '%s:/gcs' % os.path.expanduser('~/gcs')]

    mock_container_launch_by_subprocess.assert_called_once_with(
        args=[
            'docker', 'run', '--network', 'xmanager', '-p', '8080:8080', '-e',
            'c=0', '-v', 'a:b', '-v',
            '%s:/root/.config/gcloud' % os.path.expanduser('~/.config/gcloud')
        ] + expected_gcs_path_args +
        ['-it', '--entrypoint', 'bash', 'test-image'],
        check=True)


if __name__ == '__main__':
  unittest.main()
