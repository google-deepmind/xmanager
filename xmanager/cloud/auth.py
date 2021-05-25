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
"""Utility functions to authenticate with GCP."""

import os

from google import auth
from googleapiclient import discovery


def get_project_name() -> str:
  _, project = auth.default()
  return project


def get_project_number() -> str:
  rm = discovery.build('cloudresourcemanager', 'v1')
  response = rm.projects().get(projectId=get_project_name()).execute()
  return response['projectNumber']


def get_creds():
  creds, _ = auth.default()
  return creds


def get_api_key():
  # TODO: This shouldn't be necessary because we get creds from
  # auth default. Find out how to use CAIP API without needing API KEY.
  return os.environ.get('GOOGLE_CLOUD_API_KEY', None)


def get_bucket() -> str:
  bucket = os.environ.get('GOOGLE_CLOUD_BUCKET_NAME', None)
  if bucket:
    return bucket
  raise ValueError(
      '$GOOGLE_CLOUD_BUCKET_NAME is undefined. Run'
      '`export GOOGLE_CLOUD_BUCKET_NAME=<bucket-name>`, '
      'replacing <bucket-name> with a Google Cloud Storage bucket. '
      'You can create a bucket with '
      '`gsutil mb -l us-central1 gs://$GOOGLE_CLOUD_BUCKET_NAME`.')


def check_cloud():
  su = discovery.build('serviceusage', 'v1')
  su.services().enable(
      name=f'projects/{get_project_number()}/services/aiplatform.googleapis.com'
  ).execute()
  # TODO: Give AI Platform Service Agent access to gcr.io GCS
  # storage.
