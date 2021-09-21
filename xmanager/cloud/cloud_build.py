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
"""Client for interacting with Cloud Build."""
import datetime
import tarfile
import tempfile
import time
from typing import Any, Dict, Optional

from absl import flags
from docker.utils import utils as docker_utils
from google.cloud import storage
from googleapiclient import discovery
import termcolor

from xmanager.cloud import auth

_CLOUD_BUILD_TIMEOUT_SECONDS = flags.DEFINE_integer(
    'xm_cloud_build_timeout_seconds', 1200,
    'The amount of time that builds should be allowed to run, '
    'to second granularity.')
_USE_CLOUD_BUILD_CACHE = flags.DEFINE_boolean(
    'xm_use_cloud_build_cache',
    False,
    'Use Cloud Build cache to speed up the Docker build. '
    'An image with the same name tagged as :latest should exist.'
    'More details at https://cloud.google.com/cloud-build/docs/speeding-up-builds#using_a_cached_docker_image'  # pylint:disable=g-line-too-long
)

_USE_KANIKO = flags.DEFINE_boolean(
    'xm_use_kaniko', True,
    'Use kaniko backend for Cloud Build and enable caching.')
_KANIKO_CACHE_TTL = flags.DEFINE_string('xm_kaniko_cache_ttl', '336h',
                                        'Cache ttl to use for kaniko builds.')


class Client:
  """Cloud Build Client."""

  def __init__(self,
               project: Optional[str] = None,
               bucket: Optional[str] = None,
               credentials=None,
               cloud_build_timeout_seconds: Optional[int] = None,
               use_cloud_build_cache: Optional[bool] = None,
               use_kaniko: Optional[bool] = None,
               kaniko_cache_ttl: Optional[str] = None):
    """Create the Cloud Build Client.

    Args:
      project: Name of the GCP project to use for Cloud Build calls and for
        storing the data passed to Cloud Build. If not specified the project of
        the default credentials for the current environment is used.
      bucket: Bucket used to store data passed to Cloud Build. If not specified
        uses the value from the GOOGLE_CLOUD_BUCKET_NAME environment variable.
      credentials: OAuth2 Credentials to use for Cloud Build & storage calls. If
        None gets the default credentials for the current environment.
      cloud_build_timeout_seconds: The amount of time that builds should be
        allowed to run. If None defaults to `--xm_cloud_build_timeout_seconds.
      use_cloud_build_cache: Whether to use Cloud Build cache to speed up the
        Docker build. If None defaults to `--xm_use_cloud_build_cache`. An image
        with the same name tagged as :latest should exist. More details at
          https://cloud.google.com/cloud-build/docs/speeding-up-builds#using_a_cached_docker_image
      use_kaniko: Use kaniko backend for Cloud Build and enable caching. If None
        defaults to `--xm_use_kaniko`.
      kaniko_cache_ttl: Cache ttl to use for kaniko builds. If None defaults to
        `--xm_kaniko_cache_ttl`.
    """
    self.project = project or auth.get_project_name()
    self.bucket = bucket or auth.get_bucket()
    self.credentials = credentials or auth.get_creds()
    if cloud_build_timeout_seconds is None:
      cloud_build_timeout_seconds = _CLOUD_BUILD_TIMEOUT_SECONDS.value
    self.cloud_build_timeout_seconds = cloud_build_timeout_seconds
    if use_cloud_build_cache is None:
      use_cloud_build_cache = _USE_CLOUD_BUILD_CACHE.value
    self.use_cloud_build_cache = use_cloud_build_cache
    if use_kaniko is None:
      use_kaniko = _USE_KANIKO.value
    self.use_kaniko = use_kaniko
    if kaniko_cache_ttl is None:
      kaniko_cache_ttl = _KANIKO_CACHE_TTL.value
    self.kaniko_cache_ttl = kaniko_cache_ttl
    self.cloudbuild_api = None  # discovery CloudBuild v1 client

  def upload_tar_to_storage(self, archive_path: str, destination_name: str):
    storage_client = storage.Client(
        project=self.project, credentials=self.credentials)
    bucket = storage_client.bucket(self.bucket)
    blob = bucket.blob(destination_name)
    blob.upload_from_filename(archive_path)

  def build_docker_image(self, image_name: str, directory: str,
                         upload_name: str) -> str:
    """Create a Docker image via Cloud Build and push to Cloud Repository."""
    image, tag = docker_utils.parse_repository_tag(image_name)
    if not tag:
      tag = datetime.datetime.now().strftime('%Y%m%d-%H%M%S_%f')

    _, archive_path = tempfile.mkstemp(suffix='.tar.gz')
    with tarfile.open(archive_path, 'w:gz') as tar:
      tar.add(directory, '/')
    destination_name = f'{upload_name}-{tag}.tar.gz'
    self.upload_tar_to_storage(archive_path, destination_name)
    build_body = self._build_request_body(destination_name, image, tag)
    # Note: On GCP cache_discovery=True (the default) leads to ugly error
    # messages as file_cache is unavailable.
    if not self.cloudbuild_api:
      self.cloudbuild_api = discovery.build(
          'cloudbuild',
          'v1',
          credentials=self.credentials,
          cache_discovery=False)
    create_op = self.cloudbuild_api.projects().builds().create(
        projectId=self.project, body=build_body).execute()
    log_url = create_op['metadata']['build']['logUrl']
    print('Cloud Build link:', termcolor.colored(log_url, color='blue'))

    build_id = create_op['metadata']['build']['id']
    return self.wait_for_build(build_id, f'{image}:{tag}')

  def _build_request_body(self, bucket_path, image, tag) -> Dict[str, Any]:
    """Builds the Cloud Build create_build_request body."""
    body = {
        'source': {
            'storageSource': {
                'bucket': self.bucket,
                'object': bucket_path,
            },
        },
        'timeout': str(self.cloud_build_timeout_seconds) + 's',
    }
    if self.use_kaniko:
      body.update({
          'steps': [{
              'name':
                  'gcr.io/kaniko-project/executor:latest',
              'args': [
                  f'--destination={image}:{tag}', '--cache=true',
                  f'--cache-ttl={self.kaniko_cache_ttl}'
              ],
          }]
      })
    else:
      args_for_cached_image = (['--cache-from', f'{image}:latest']
                               if self.use_cloud_build_cache else [])
      body.update({
          'steps': [{
              'name':
                  'gcr.io/cloud-builders/docker',
              'args':
                  ['build', '-t', f'{image}:{tag}', '-t', f'{image}:latest'] +
                  args_for_cached_image + ['.'],
          }],
          'options': {
              'machineType': 'E2_HIGHCPU_32'
          },
          'images': [image]
      })
    return body

  def wait_for_build(self, build_id: str, kaniko_image: str) -> str:
    """Waits for build to finish and return the image URI of the result."""
    backoff = 30  # seconds
    while True:
      time.sleep(backoff)
      result = self.cloudbuild_api.projects().builds().get(
          projectId=self.project, id=build_id).execute()
      status = result['status']
      print('Cloud Build status:', status)

      if status == 'SUCCESS':
        image_uri = kaniko_image
        if not self.use_kaniko:
          # Note: Not sure if this is needed. Could we always use the uri above?
          image = result['results']['images'][0]
          image_uri = f'{image["name"]}@{image["digest"]}'
        break
      elif status == 'FAILURE':
        print('Build FAILED. See logs for more information:',
              termcolor.colored(result['logUrl'], color='red'))
        raise RuntimeError('Build FAILED.')
      elif status == 'QUEUED' or status == 'WORKING':
        continue
      elif status == 'INTERNAL_ERROR' or status == 'CANCELLED':
        print('Cloud Build tool failure. Status:', status)
        raise RuntimeError('Cloud Build tool failed. Try again.')
      else:
        print('Build not complete. See logs for more information:',
              termcolor.colored(result['logUrl'], color='red'))
        raise RuntimeError('Build FAILED.')

    print('Your image URI is:', termcolor.colored(image_uri, color='blue'))
    print('You can run your image locally via:\n' +
          termcolor.colored('docker run ' + image_uri, color='green'))
    return image_uri
