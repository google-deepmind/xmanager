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
"""Helpers to support GCS-based output dir.

For the code running on Google Cloud a GCS directory should be used to store
data.

A GCS name should have a particular format 'gs://bucket/directory'. For XCloud
we provide a shared bucket which can be used by all researchers. To ensure the
format of the name is correct, and to suggest reasonable defaults for it, this
file provides a flag and several helper functions.
"""

import datetime
import getpass
import os

from absl import app
from absl import flags

_GCS_PATH = flags.DEFINE_string(
    'xm_gcs_path', None, 'A GCS directory within a bucket to store output '
    '(in gs://bucket/directory format).')

_GS_PREFIX = 'gs://'
_GCS_PREFIX = '/gcs/'


_default_bucket_name = 'xcloud-shared'


def set_default_bucket(bucket: str) -> None:
  """Helper to allow sharing this code with XCloud v1. Not for general use."""
  # TODO: Remove once v1 will be shut down.
  global _default_bucket_name
  _default_bucket_name = bucket


def suggestion(project_name: str) -> str:
  """Returns a suggested GCS dir name for the given @project_name."""
  return os.path.join(
      _GS_PREFIX, _default_bucket_name, getpass.getuser(),
      project_name + '-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))


def get_gcs_path_or_suggestion(project_name: str) -> str:
  """Returns a value passed in the --xm_gcs_path flag or a suggested valid path.

  Args:
    project_name: a project name used to generate suggested GCS path.

  Returns:
    If the --xm_gcs_path flag is empty, returns a reasonable suggestion.
    If the --xm_gcs_path contains invalid value, raise an error.
    Otherwise, returns a flag value.
  """
  if not _GCS_PATH.value:
    return suggestion(project_name)
  elif not is_gcs_path(_GCS_PATH.value):
    raise app.UsageError(
        '--xm_gcs_path not in gs://bucket/directory or /gcs/path format. ' +
        f'Suggestion: --xm_gcs_path={suggestion(project_name)}')
  return str(_GCS_PATH.value)


def get_gcs_path_or_fail(project_name: str) -> str:
  """Returns value passed in the --xm_gcs_path flag; fails if nothing is passed.

  Args:
    project_name: a project name used to generate suggested GCS path.

  Returns:
    If the --xm_gcs_path flag is empty, or contains invalid value, raise an
    error. Otherwise, returns a flag value.
  """
  if not _GCS_PATH.value:
    raise app.UsageError('--xm_gcs_path is missing. Suggestion: ' +
                         f'--xm_gcs_path={suggestion(project_name)}')
  elif not is_gcs_path(_GCS_PATH.value):
    raise app.UsageError(
        '--xm_gcs_path not in gs://bucket/directory or /gcs/path format. ' +
        f'Suggestion: --xm_gcs_path={suggestion(project_name)}')
  return str(_GCS_PATH.value)


def is_gs_path(path: str) -> bool:
  """Given the path, checks whether it is a valid Google Storage URL.

  Args:
    path: a path.

  Returns:
    True iff a path starts with the 'gs://' prefix.
  """
  return path.startswith(_GS_PREFIX)


def is_gcs_fuse_path(path: str) -> bool:
  """Given the path, checks whether it is a valid gcs_fuse path.

  Args:
    path: a path.

  Returns:
    True iff a path starts with the '/gcs/' prefix.
  """
  return path.startswith(_GCS_PREFIX)


def is_gcs_path(path: str) -> bool:
  """Given the path, checks whether it is a valid GCS URL supported by XCloud.

  Args:
    path: a path.

  Returns:
    True iff a path starts with either 'gs://' or '/gcs/' prefix.
  """
  return is_gs_path(path) or is_gcs_fuse_path(path)


def _gcs_path_no_prefix(path: str) -> str:
  """Given the GCS path in gs://bucket/directory format, strips 'gs://' from it.

  Args:
    path: GCS path in gs://bucket/directory format.

  Returns:
    Path without 'gs://' prefix.
  """
  if is_gs_path(path):
    return path[len(_GS_PREFIX):]
  if is_gcs_fuse_path(path):
    return path[len(_GCS_PREFIX):]
  raise ValueError(
      f'Path not in gs://bucket/directory or /gcs/path format: {path}')


# Exposed for testing.
gcp_website_url = 'https://console.cloud.google.com'


def get_gcs_url(path: str) -> str:
  """Given the GCS path, provides a GCS URL to access it.

  Args:
    path: GCS path in gs://bucket/directory format.

  Returns:
    GCS URL to access path.
  """
  no_prefix = _gcs_path_no_prefix(path)
  return f'{gcp_website_url}/storage/browser/{no_prefix}'


def get_gcs_fuse_path(path: str) -> str:
  """Given the GCS path, provides corresponding Cloud Storage FUSE path.

  See https://cloud.google.com/storage/docs/gcs-fuse for details.

  Args:
    path: GCS path in gs://bucket/directory format.

  Returns:
    Path in the /gcs/bucket/directory format.
  """
  return _GCS_PREFIX + _gcs_path_no_prefix(path)


def get_gs_path(path: str) -> str:
  """Given the GCS path, provides corresponding Cloud Storage URL.

  See https://cloud.google.com/storage/docs/gcs-fuse for details.

  Args:
    path: GCS path in /gcs/bucket/directory format.

  Returns:
    Path in the gs://bucket/directory format.
  """
  return _GS_PREFIX + _gcs_path_no_prefix(path)
