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
"""Tests for cloud_build."""
from unittest import mock
from absl.testing import absltest

from xmanager.cloud import cloud_build


class CloudBuildTest(absltest.TestCase):

  def test_build_request_body(self):
    client = cloud_build.Client(
        'my-project',
        'my-bucket',
        mock.Mock(),
        use_kaniko=False,
        use_cloud_build_cache=False)
    image = client._build_request_body('path/to/project', 'my-image', 'live')
    self.assertEqual(
        image, {
            'images': ['my-image'],
            'options': {
                'machineType': 'E2_HIGHCPU_32'
            },
            'source': {
                'storageSource': {
                    'bucket': 'my-bucket',
                    'object': 'path/to/project',
                },
            },
            'steps': [{
                'args': [
                    'build', '-t', 'my-image:live', '-t', 'my-image:latest', '.'
                ],
                'name': 'gcr.io/cloud-builders/docker',
            }],
            'timeout': '1200s'
        })

  def test_build_request_body_use_kaniko(self):
    client = cloud_build.Client(
        'my-project',
        'my-bucket',
        mock.Mock(),
        use_kaniko=True,
        use_cloud_build_cache=False)
    image = client._build_request_body('path/to/project', 'my-image', 'live')
    self.assertEqual(
        image, {
            'source': {
                'storageSource': {
                    'bucket': 'my-bucket',
                    'object': 'path/to/project',
                },
            },
            'steps': [{
                'args': [
                    '--destination=my-image:live',
                    '--destination=my-image:latest',
                    '--cache=true',
                    '--cache-ttl=336h',
                ],
                'name': 'gcr.io/kaniko-project/executor:latest',
            }],
            'timeout': '1200s'
        })

  def test_build_request_body_use_build_cache(self):
    client = cloud_build.Client(
        'my-project',
        'my-bucket',
        mock.Mock(),
        use_kaniko=False,
        use_cloud_build_cache=True)
    image = client._build_request_body('path/to/project', 'my-image', 'live')
    self.assertEqual(
        image, {
            'images': ['my-image'],
            'options': {
                'machineType': 'E2_HIGHCPU_32'
            },
            'source': {
                'storageSource': {
                    'bucket': 'my-bucket',
                    'object': 'path/to/project',
                },
            },
            'steps': [{
                'args': [
                    'build',
                    '-t',
                    'my-image:live',
                    '-t',
                    'my-image:latest',
                    '--cache-from',
                    'my-image:latest',
                    '.',
                ],
                'name': 'gcr.io/cloud-builders/docker',
            }],
            'timeout': '1200s'
        })


if __name__ == '__main__':
  absltest.main()
