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
"""Tests for xmanager.cloud.vertex."""
import datetime
import os
import unittest
from unittest import mock
import io
from contextlib import redirect_stdout

from absl.testing import parameterized
from google import auth
from google.auth import credentials
from google.cloud import aiplatform
from google.cloud import aiplatform_v1 as aip_v1
from google.cloud.aiplatform import utils as aip_utils

from xmanager import xm
from xmanager.cloud import auth as xm_auth
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import executors as local_executors

from xmanager.cloud import vertex  # pylint: disable=g-bad-import-order


class VertexTest(parameterized.TestCase):

  @mock.patch.object(xm_auth, 'get_service_account')
  @mock.patch.object(auth, 'default')
  def test_launch(self, mock_creds, mock_sa):
    os.environ['GOOGLE_CLOUD_BUCKET_NAME'] = 'test-bucket'
    creds = credentials.AnonymousCredentials()
    mock_creds.return_value = (creds, 'test-project')
    mock_sa.return_value = 'test-sa'

    client = vertex.Client('test-project', 'us-central1')
    job = xm.Job(
        name='test-job',
        executable=local_executables.GoogleContainerRegistryImage(
            name='test-image',
            image_path='image-path',
            args=xm.SequentialArgs.from_collection({'a': 1}),
        ),
        executor=local_executors.Vertex(xm.JobRequirements(cpu=1, ram=1, t4=2)),
        args={'b': 2, 'c': 3},
    )

    expected_call = {
        'parent': 'projects/test-project/locations/us-central1',
        'custom_job': aip_v1.CustomJob(
            display_name='test-experiment',
            job_spec=aip_v1.CustomJobSpec(
                worker_pool_specs=[
                    aip_v1.WorkerPoolSpec(
                        machine_spec=aip_v1.MachineSpec(
                            machine_type='n1-highmem-2',
                            accelerator_type='NVIDIA_TESLA_T4',
                            accelerator_count=2,
                        ),
                        replica_count=1,
                        container_spec=aip_v1.ContainerSpec(
                            image_uri='image-path',
                            args=['--a=1', '--b=2', '--c=3'],
                        ),
                    )
                ],
                service_account='test-sa',
                base_output_directory=aip_v1.GcsDestination(
                    output_uri_prefix='gs://test-bucket/aiplatform-custom-job-2022-01-01-00:00:00.000',
                ),
                enable_web_access=True,
            ),
        ),
        'timeout': None,
    }

    timestamp = datetime.datetime.strptime('2022/1/1', '%Y/%m/%d')
    with mock.patch.object(
        datetime, 'datetime'
    ) as mock_timestamp, mock.patch.object(
        aip_utils.ClientWithOverride, 'WrappedClient'
    ) as job_client, mock.patch.object(
        aiplatform.CustomJob, 'resource_name', new_callable=mock.PropertyMock
    ) as name, mock.patch.object(
        aiplatform.CustomJob, '_dashboard_uri'
    ):
      mock_timestamp.now.return_value = timestamp
      name.return_value = 'test-resource-name'
      client.launch('test-experiment', [job])
      job_client.return_value.create_custom_job.assert_called_once_with(  # pytype: disable=attribute-error  # py39-upgrade
          **expected_call
      )

  def test_get_machine_spec_default(self):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(machine_spec, {'machine_type': 'n1-standard-4'})

  def test_get_machine_spec_cpu(self):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(cpu=20, ram=40 * xm.GiB)
        ),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(machine_spec, {'machine_type': 'n1-highcpu-64'})

  def test_get_machine_spec_gpu(self):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(p100=2)
        ),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(
        machine_spec,
        {
            'machine_type': 'n1-standard-4',
            'accelerator_type': vertex.aip_v1.AcceleratorType.NVIDIA_TESLA_P100,
            'accelerator_count': 2,
        },
    )

  def test_get_machine_spec_a100(self):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(a100=2)
        ),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(
        machine_spec,
        {
            'machine_type': 'a2-highgpu-2g',
            'accelerator_type': vertex.aip_v1.AcceleratorType.NVIDIA_TESLA_A100,
            'accelerator_count': 2,
        },
    )

  @parameterized.parameters(
    {'cpus': 4, 'gpus': 1, 'expected': 'g2-standard-4'},
    {'cpus': 8, 'gpus': 1, 'expected': 'g2-standard-8'},
    {'cpus': 12, 'gpus': 1, 'expected': 'g2-standard-12'},
    {'cpus': 16, 'gpus': 1, 'expected': 'g2-standard-16'},
    {'cpus': 32, 'gpus': 1, 'expected': 'g2-standard-32'},
    {'cpus': 24, 'gpus': 2, 'expected': 'g2-standard-24'},
    {'cpus': 48, 'gpus': 4, 'expected': 'g2-standard-48'},
    {'cpus': 96, 'gpus': 8, 'expected': 'g2-standard-96'},
  )
  def test_get_machine_spec_l4(self, cpus, gpus, expected):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(l4=gpus, cpu=cpus)
        ),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(
        machine_spec,
        {
            'machine_type': expected,
            'accelerator_type': vertex.aip_v1.AcceleratorType.NVIDIA_L4,
            'accelerator_count': gpus,
        },
    )

  @parameterized.parameters(
    {'cpus': 3, 'gpus': 1},
    {'cpus': 4, 'gpus': 2},
    {'cpus': 25, 'gpus': 2},
    {'cpus': 41, 'gpus': 4},
    {'cpus': 48, 'gpus': 8},
  )
  def test_get_machine_spec_l4_failure(self, cpus, gpus):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(l4=gpus, cpu=cpus)
        ),
        args={},
    )
    f = io.StringIO()
    with redirect_stdout(f), self.assertRaises(ValueError) as cm:
      vertex.get_machine_spec(job)

    self.assertIn('Available L4 machine types', f.getvalue())
    self.assertIn(
        f'l4={gpus} with {cpus}.0 CPUs does not fit in any valid machine type.',
        str(cm.exception),
    )

  def test_get_machine_spec_tpu(self):
    job = xm.Job(
        executable=local_executables.GoogleContainerRegistryImage('name', ''),
        executor=local_executors.Vertex(
            requirements=xm.JobRequirements(tpu_v3=8)
        ),
        args={},
    )
    machine_spec = vertex.get_machine_spec(job)
    self.assertDictEqual(
        machine_spec,
        {
            'machine_type': 'cloud-tpu',
            'accelerator_type': vertex.aip_v1.AcceleratorType.TPU_V3,
            'accelerator_count': 8,
        },
    )

  def test_cpu_ram_to_machine_type_exact(self):
    self.assertEqual(
        'n1-standard-16', vertex.cpu_ram_to_machine_type(16, 60 * xm.GiB)
    )

  def test_cpu_ram_to_machine_type_highmem(self):
    self.assertEqual(
        'n1-highmem-64', vertex.cpu_ram_to_machine_type(1, 415 * xm.GiB)
    )

  def test_cpu_ram_to_machine_type_mem_only(self):
    self.assertEqual(
        'n1-highmem-64', vertex.cpu_ram_to_machine_type(None, 415 * xm.GiB)
    )

  def test_cpu_ram_to_machine_type_highcpu(self):
    self.assertEqual(
        'n1-highcpu-64', vertex.cpu_ram_to_machine_type(63, 1 * xm.GiB)
    )

  def test_cpu_ram_to_machine_type_cpu_only(self):
    self.assertEqual('n1-highcpu-64', vertex.cpu_ram_to_machine_type(63, None))

  def test_cpu_ram_to_machine_type_too_high(self):
    with self.assertRaises(ValueError):
      vertex.cpu_ram_to_machine_type(1000, 1000)


if __name__ == '__main__':
  unittest.main()
