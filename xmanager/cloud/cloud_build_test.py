"""Tests for cloud_build."""
from absl.testing import absltest

from xmanager.cloud import cloud_build


class CloudBuildTest(absltest.TestCase):

  def test_build_request_body(self):
    client = cloud_build.Client(
        'my-project',
        'my-bucket',
        'fake-creds',
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
        'fake-creds',
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
        'fake-creds',
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
