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


def create_test_job(
    gpu_count: int, interactive: bool = False, mount_gcs_path: bool = True
) -> xm.Job:
  return xm.Job(
      name='test-job',
      executable=local_executables.LoadedContainerImage(
          name='test',
          image_id='test-image',
          args=xm.SequentialArgs.from_collection({'a': 1}),
          env_vars={'c': '0'},
      ),
      executor=local_executors.Local(
          requirements=xm.JobRequirements(local_gpu=gpu_count),
          docker_options=local_executors.DockerOptions(
              ports={8080: 8080},
              volumes={'a': 'b'},
              interactive=interactive,
              mount_gcs_path=mount_gcs_path,
          ),
      ),
  )


class ExecutionTest(unittest.IsolatedAsyncioTestCase, parameterized.TestCase):
  """Tests for xm_local.execution (currently only for container launches)."""

  async def asyncSetUp(self):
    # Force flag initialization to avoid errors
    flags.FLAGS(sys.argv)

  @parameterized.product(
      interactive=[True, False],
      mount_gcs_path=[True, False],
      gcs_dir_exists=[True, False],
      gpu_count=[0, 1, 4],
  )
  @mock.patch.object(
      docker_adapter.DockerAdapter, 'run_container', return_value=None
  )
  async def test_container_launch_dispatcher(
      self,
      mock_run_container,
      interactive,
      mount_gcs_path,
      gcs_dir_exists,
      gpu_count,
  ):
    """Tests if the container launch dispatcher is called correctly when using `xm_local.execution.launch`."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True

    job = create_test_job(
        interactive=interactive,
        mount_gcs_path=mount_gcs_path,
        gpu_count=gpu_count,
    )

    mock_gcs_dir_existence = lambda path: (  # pylint:disable=g-long-lambda
        gcs_dir_exists and path.endswith('/gcs')
    )

    with mock.patch.object(
        docker, 'from_env', return_value=mock_docker_client
    ), mock.patch.object(
        os.path, 'isdir', side_effect=mock_gcs_dir_existence
    ), mock.patch.object(
        subprocess, 'check_output', return_value=True
    ):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_gcs_volume = {os.path.expanduser('~/gcs'): '/gcs'}
    mock_run_container.assert_called_once_with(
        name='test-job',
        image_id='test-image',
        network='xmanager',
        args=['--a=1'],
        env_vars={'c': '0'},
        ports={8080: 8080},
        volumes=(
            {
                'a': 'b',
                os.path.expanduser('~/.config/gcloud'): '/root/.config/gcloud',
            }
            | (expected_gcs_volume if mount_gcs_path and gcs_dir_exists else {})
        ),
        gpu_count=gpu_count,
        interactive=interactive,
    )

  @parameterized.product(
      mount_gcs_path=[True, False],
      gcs_dir_exists=[True, False],
      gpu_count=[0, 1, 4],
  )
  @mock.patch.object(
      docker.models.containers.ContainerCollection, 'run', return_value=None
  )
  async def test_container_launch_by_client(
      self, mock_client_run, mount_gcs_path, gcs_dir_exists, gpu_count
  ):
    """Tests if the Docker Python client launches containers correctly when using `xm_local.execution.launch`."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True
    mock_docker_client.containers = (
        docker.models.containers.ContainerCollection(None)
    )

    job = create_test_job(
        interactive=False, mount_gcs_path=mount_gcs_path, gpu_count=gpu_count
    )
    mock_gcs_dir_existence = lambda path: (  # pylint:disable=g-long-lambda
        gcs_dir_exists and path.endswith('/gcs')
    )

    with mock.patch.object(
        docker, 'from_env', return_value=mock_docker_client
    ), mock.patch.object(
        os.path, 'isdir', side_effect=mock_gcs_dir_existence
    ), mock.patch.object(
        subprocess, 'check_output', return_value=True
    ):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_gcs_volume = {
        os.path.expanduser('~/gcs'): {'bind': '/gcs', 'mode': 'rw'}
    }
    mock_client_run.assert_called_once_with(
        'test-image',
        name='test-job',
        hostname='test-job',
        network='xmanager',
        detach=True,
        remove=True,
        command=['--a=1'],
        environment={'c': '0'},
        ports={8080: 8080},
        volumes=(
            {
                'a': {'bind': 'b', 'mode': 'rw'},
                os.path.expanduser('~/.config/gcloud'): {
                    'bind': '/root/.config/gcloud',
                    'mode': 'rw',
                },
            }
            | (expected_gcs_volume if mount_gcs_path and gcs_dir_exists else {})
        ),
        runtime='nvidia' if gpu_count > 0 else None,
        device_requests=[  # pylint:disable=g-long-ternary
            docker.types.DeviceRequest(count=gpu_count, capabilities=[['gpu']])
        ]
        if gpu_count > 0
        else None,
    )

  @parameterized.product(
      mount_gcs_path=[True, False],
      gcs_dir_exists=[True, False],
      gpu_count=[0, 1, 4],
  )
  @mock.patch.object(subprocess, 'run', return_value=None)
  async def test_container_launch_by_subprocess(
      self,
      mock_container_launch_by_subprocess,
      mount_gcs_path,
      gcs_dir_exists,
      gpu_count,
  ):
    """Tests if the Docker subprocesses are created correctly when using `xm_local.execution.launch."""

    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True

    job = create_test_job(
        interactive=True, mount_gcs_path=mount_gcs_path, gpu_count=gpu_count
    )
    mock_gcs_dir_existence = lambda path: (  # pylint:disable=g-long-lambda
        gcs_dir_exists and path.endswith('/gcs')
    )

    with mock.patch.object(
        docker, 'from_env', return_value=mock_docker_client
    ), mock.patch.object(
        os.path, 'isdir', side_effect=mock_gcs_dir_existence
    ), mock.patch.object(
        subprocess, 'check_output', return_value=True
    ):
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

    expected_gcs_path_args = []
    if mount_gcs_path and gcs_dir_exists:
      expected_gcs_path_args = ['-v', '%s:/gcs' % os.path.expanduser('~/gcs')]

    expected_gpu_args = []
    if gpu_count > 0:
      expected_gpu_args = ['--gpus', str(gpu_count), '--runtime', 'nvidia']

    mock_container_launch_by_subprocess.assert_called_once_with(
        args=[
            'docker',
            'run',
            '--network',
            'xmanager',
            '-p',
            '8080:8080',
            '-e',
            'c=0',
            '-v',
            'a:b',
            '-v',
            '%s:/root/.config/gcloud' % os.path.expanduser('~/.config/gcloud'),
        ]
        + expected_gcs_path_args
        + expected_gpu_args
        + ['-it', '--entrypoint', 'bash', 'test-image'],
        check=True,
    )

  @parameterized.product(interactive=[True, False], gpu_count=[0, 1, 4])
  @mock.patch.object(
      docker_adapter.DockerAdapter, 'run_container', return_value=None
  )
  async def test_no_nvidia_smi_launch(
      self, mock_run_container, interactive, gpu_count
  ):
    mock_docker_client = mock.Mock()
    mock_docker_client.has_network.return_value = True

    job = create_test_job(
        interactive=interactive, mount_gcs_path=True, gpu_count=gpu_count
    )

    if gpu_count > 0:
      with mock.patch.object(
          subprocess, 'check_output', side_effect=Exception()
      ), self.assertRaises(RuntimeError):
        await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

      mock_run_container.assert_not_called()
    else:
      await execution.launch(lambda x: x, job_group=xm.JobGroup(test_job=job))

      mock_run_container.assert_called_once_with(
          name='test-job',
          image_id='test-image',
          network='xmanager',
          args=['--a=1'],
          env_vars={'c': '0'},
          ports={8080: 8080},
          volumes={
              'a': 'b',
              os.path.expanduser('~/.config/gcloud'): '/root/.config/gcloud',
          },
          gpu_count=gpu_count,
          interactive=interactive,
      )


if __name__ == '__main__':
  unittest.main()
