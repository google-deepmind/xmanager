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
"""Tests for the xmanager.contrib.gcs module."""

import getpass

from absl import app
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from xmanager import xm_abc
from xmanager.contrib import gcs

# Error patterns.
_GCS_PATH_ERROR = (
    '--xm_gcs_path not in gs://bucket/directory or /gcs/path format.'
)
_PATH_ERROR = 'Path not in gs://bucket/directory or /gcs/path format'

_MOUNT_POINT_ERROR = 'Mount point is not a valid path'

_EXPECTED_VOLUME = xm_abc.GcsVolume(
    bucket='xcloud-shared',
    directory_path='directory_path/1/2',
    mount_path='/mount_path',
)


class GcsTest(parameterized.TestCase):

  def test_gcs_path_empty_flag(self):
    with self.assertRaisesRegex(app.UsageError, '--xm_gcs_path is missing'):
      gcs.get_gcs_path_or_fail('project')

  def test_gcs_path_correct_value(self):
    with flagsaver.flagsaver(xm_gcs_path='gs://bucket/dir'):
      self.assertEqual(gcs.get_gcs_path_or_fail('project'), 'gs://bucket/dir')

  def test_gcs_path_incorrect_value(self):
    with flagsaver.flagsaver(xm_gcs_path='file://dir'):
      with self.assertRaisesRegex(app.UsageError, _GCS_PATH_ERROR):
        gcs.get_gcs_path_or_fail('project')

  @parameterized.named_parameters(
      (
          'gs_prefix',
          'gs://xcloud-shared/directory_path/1/2',
          '/mount_path',
          _EXPECTED_VOLUME,
          None,
      ),
      (
          'gcs_prefix',
          '/gcs/xcloud-shared/directory_path/1/2',
          '/mount_path',
          _EXPECTED_VOLUME,
          None,
      ),
      (
          'no_bucket_path',
          '',
          '/mount_path',
          None,
          _PATH_ERROR,
      ),
      (
          'no_mount_path',
          '/gcs/xcloud-shared/directory_path/1/2',
          '',
          None,
          _MOUNT_POINT_ERROR,
      ),
      (
          'invalid_prefix',
          'xcloud-shared/directory_path/1/2',
          '/mount_path',
          None,
          _PATH_ERROR,
      ),
      (
          'invalid_mount_point',
          'gs://xcloud-shared/directory_path/1/2',
          'mount_path',
          None,
          _MOUNT_POINT_ERROR,
      ),
  )
  def test_path_to_volume(self, path, mount_point, expected, error):
    if error is not None:
      with self.assertRaisesRegex(ValueError, error):
        gcs.path_to_volume(path, mount_point)
    else:
      self.assertEqual(gcs.path_to_volume(path, mount_point), expected)

  # pylint: disable=bad-whitespace
  @parameterized.named_parameters(
      ('gs_long',      'gs://a/b/c', True,  False, True),
      ('gs_short',     'gs://d/e',   True,  False, True),
      ('gs_invalid',   'gs:/d/e',    False, False, False),
      ('fuse_long',    '/gcs/a/b/c', False, True,  True),
      ('fuse_short',   '/gcs/d/e',   False, True,  True),
      ('fuse_invalid', '/gcsc/d/e',  False, False, False),
      ('invalid',      'a/b/f',      False, False, False),
  )  # pyformat:disable
  # pylint: enable=bad-whitespace
  def test_is_path(self, path, expected_gs, expected_fuse, expected_gcs):
    self.assertEqual(gcs.is_gs_path(path), expected_gs)
    self.assertEqual(gcs.is_gcs_fuse_path(path), expected_fuse)
    self.assertEqual(gcs.is_gcs_path(path), expected_gcs)

  def test_get_gcs_url(self):
    self.assertEqual(
        gcs.get_gcs_url('gs://a/b/c'),
        f'{gcs.gcp_website_url}/storage/browser/a/b/c',
    )
    self.assertEqual(
        gcs.get_gcs_url('gs://d/e'),
        f'{gcs.gcp_website_url}/storage/browser/d/e',
    )
    self.assertEqual(
        gcs.get_gcs_url('/gcs/a/b/c'),
        f'{gcs.gcp_website_url}/storage/browser/a/b/c',
    )
    self.assertEqual(
        gcs.get_gcs_url('/gcs/d/e'),
        f'{gcs.gcp_website_url}/storage/browser/d/e',
    )
    with self.assertRaisesRegex(ValueError, _PATH_ERROR):
      gcs.get_gcs_url('a/b/f')

  def test_get_gcs_fuse_path(self):
    self.assertEqual(gcs.get_gcs_fuse_path('gs://a/b/c'), '/gcs/a/b/c')
    self.assertEqual(gcs.get_gcs_fuse_path('gs://d/e'), '/gcs/d/e')
    self.assertEqual(gcs.get_gcs_fuse_path('/gcs/a/b/c'), '/gcs/a/b/c')
    self.assertEqual(gcs.get_gcs_fuse_path('/gcs/d/e'), '/gcs/d/e')
    with self.assertRaisesRegex(ValueError, _PATH_ERROR):
      gcs.get_gcs_fuse_path('a/b/f')

  def test_get_gs_path(self):
    self.assertEqual(gcs.get_gs_path('gs://a/b/c'), 'gs://a/b/c')
    self.assertEqual(gcs.get_gs_path('gs://d/e'), 'gs://d/e')
    self.assertEqual(gcs.get_gs_path('/gcs/a/b/c'), 'gs://a/b/c')
    self.assertEqual(gcs.get_gs_path('/gcs/d/e'), 'gs://d/e')
    with self.assertRaisesRegex(ValueError, _PATH_ERROR):
      gcs.get_gcs_fuse_path('a/b/f')


if __name__ == '__main__':
  absltest.main()
