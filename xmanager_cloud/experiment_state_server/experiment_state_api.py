# Copyright 2025 Google LLC
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
"""Python wrapper for the Experiment State Server API."""

import functools
import logging
import os
from typing import Any, Callable

import google.auth
from google.auth.transport import requests
from google.oauth2 import id_token
import grpc

from google.longrunning import operations_pb2

from xmanager_cloud.experiment_state_server.proto import api_pb2 as experiment_state_service_pb2
from xmanager_cloud.experiment_state_server.proto import api_pb2_grpc as experiment_state_service_pb2_grpc
from xmanager_cloud.experiment_state_server.proto import artifact_pb2
from xmanager_cloud.experiment_state_server.proto import experiment_pb2
from xmanager_cloud.experiment_state_server.proto import work_unit_pb2


_XMANAGER_ENDPOINT = 'dns:///grpc.api.alpha.example.com'
_CHANNEL_READY_TIMEOUT_SEC = 10


def _get_xmanager_endpoint() -> str:
  if env_var := os.environ.get('XMANAGER_ENDPOINT'):
    return env_var
  return _XMANAGER_ENDPOINT


def get_current_user_email() -> str:
  """Retrieves the email of the current authenticated user.

  This function attempts to get the user's email from the credentials,
  prioritizing service account email, then ID token claims, and finally
  the OpenID Connect userinfo endpoint.

  Returns:
    The email address of the current user.

  Raises:
    RuntimeError: If the user email cannot be retrieved.
  """
  try:
    scopes = [
        'openid',
        'https://www.googleapis.com/auth/userinfo.email',
    ]
    creds, _ = google.auth.default(scopes=scopes)

    request = google.auth.transport.requests.Request()
    if not creds.valid:
      creds.refresh(request)

    if hasattr(creds, 'service_account_email'):
      logging.info('Using service account email.')
      return creds.service_account_email

    if getattr(creds, 'id_token', None):
      try:
        claims = id_token.verify_oauth2_token(creds.id_token, request)
      except ValueError:
        # If id_token is invalid, fall back to userinfo endpoint.
        pass
      else:
        email = claims.get('email')
        if email:
          logging.info('Using ID token email.')
          return email

    with requests.AuthorizedSession(creds) as authed_session:
      response = authed_session.get(
          'https://openidconnect.googleapis.com/v1/userinfo'
      )

      if response.status_code != 200:
        raise RuntimeError(
            f'Failed to retrieve userinfo: {response.status_code} '
            f'{response.text}'
        )
      userinfo_email = response.json().get('email')
      if userinfo_email:
        logging.info('Using userinfo endpoint email.')
        return userinfo_email
      else:
        raise RuntimeError('Email not found in userinfo response.')
  except Exception as e:
    raise RuntimeError('Failed to get current user email') from e


class AuditMetadataInterceptor(grpc.UnaryUnaryClientInterceptor):
  """gRPC client interceptor to audit metadata."""

  def __init__(self, user_email: str):
    self._user_email = user_email

  def intercept_unary_unary(
      self,
      continuation: Callable[[Any, Any], Any],
      client_call_details: Any,
      request: Any,
  ) -> Any:
    metadata = list(client_call_details.metadata or [])
    metadata.append(('x-goog-user-email', self._user_email))

    return continuation(
        client_call_details._replace(metadata=metadata), request
    )


def _create_experiment_state_server_stub(
    endpoint: str,
) -> tuple[
    experiment_state_service_pb2_grpc.ExperimentStateServiceStub,
    grpc.Channel,
]:
  """Create a gRPC stub and channel for the Experiment State Server."""
  iap_client_id = os.environ.get('IAP_CLIENT_ID')
  if not iap_client_id:
    print(
        'IAP_CLIENT_ID not set. It should be the OAuth client ID used by IAP '
        'for your backend.'
    )
    iap_client_id = input('Enter IAP_CLIENT_ID: ')
  client_sa = os.environ.get('XMC_CLIENT_SA')
  if not client_sa:
    print(
        'XMC_CLIENT_SA not set. It should be the email of the service account '
        'to impersonate (e.g., '
        'xmc-client-username@my-project-id.iam.gserviceaccount.com).'
    )
    client_sa = input('Enter XMC_CLIENT_SA: ')

  try:
    credentials, _ = google.auth.default()
    authed_session = google.auth.transport.requests.AuthorizedSession(
        credentials
    )
    url = (
        'https://iamcredentials.googleapis.com/v1/projects/-/'
        f'serviceAccounts/{client_sa}:generateIdToken'
    )
    body = {'audience': iap_client_id, 'includeEmail': True}
    response = authed_session.post(url, json=body)
    response.raise_for_status()
    open_id_connect_token = response.json()['token']
  except Exception as e:
    raise RuntimeError('Failed to get identity token via google-auth') from e
  else:

    user_email = get_current_user_email()

    channel_creds = grpc.ssl_channel_credentials()
    call_creds = grpc.access_token_call_credentials(open_id_connect_token)
    composite_creds = grpc.composite_channel_credentials(
        channel_creds, call_creds
    )
    channel = grpc.secure_channel(endpoint, composite_creds)

    audit_interceptor = AuditMetadataInterceptor(user_email)
    intercepted_channel = grpc.intercept_channel(channel, audit_interceptor)

  grpc.channel_ready_future(intercepted_channel).result(
      _CHANNEL_READY_TIMEOUT_SEC
  )
  return (
      experiment_state_service_pb2_grpc.ExperimentStateServiceStub(
          intercepted_channel
      ),
      intercepted_channel,
  )


class ExperimentStateGRpcApi:
  """Convenience wrapper for the Experiment State Server gRPC API.

  The definition of the underlying gRPC API can be found here:
  TODO: (internal issue) - Replace with a link to github once we open source
  the backend.
  (internal link)
  """

  def __init__(self):
    self._stub, self._channel = _create_experiment_state_server_stub(
        _get_xmanager_endpoint()
    )

  def create_experiment(
      self, request: experiment_state_service_pb2.CreateExperimentRequest
  ) -> experiment_pb2.Experiment:
    return self._stub.CreateExperiment(request)

  def create_work_unit(
      self, request: experiment_state_service_pb2.CreateWorkUnitRequest
  ) -> operations_pb2.Operation:
    return self._stub.CreateWorkUnit(request)

  def get_experiment(
      self, request: experiment_state_service_pb2.GetExperimentRequest
  ) -> experiment_pb2.Experiment:
    return self._stub.GetExperiment(request)

  def list_experiments(
      self, request: experiment_state_service_pb2.ListExperimentsRequest
  ) -> experiment_state_service_pb2.ListExperimentsResponse:
    return self._stub.ListExperiments(request)

  def search_experiments(
      self, request: experiment_state_service_pb2.ListExperimentsRequest
  ) -> experiment_state_service_pb2.ListExperimentsResponse:
    return self._stub.SearchExperiments(request)

  def get_work_unit(
      self, request: experiment_state_service_pb2.GetWorkUnitRequest
  ) -> work_unit_pb2.WorkUnit:
    return self._stub.GetWorkUnit(request)

  def list_work_units(
      self, request: experiment_state_service_pb2.ListWorkUnitsRequest
  ) -> experiment_state_service_pb2.ListWorkUnitsResponse:
    return self._stub.ListWorkUnits(request)

  def search_work_units(
      self, request: experiment_state_service_pb2.ListWorkUnitsRequest
  ) -> experiment_state_service_pb2.ListWorkUnitsResponse:
    return self._stub.SearchWorkUnits(request)

  def update_experiment_launch_state(
      self,
      request: experiment_state_service_pb2.UpdateExperimentLaunchStateRequest,
  ) -> experiment_pb2.Experiment:
    return self._stub.UpdateExperimentLaunchState(request)

  def update_experiment(
      self, request: experiment_state_service_pb2.UpdateExperimentRequest
  ) -> experiment_pb2.Experiment:
    return self._stub.UpdateExperiment(request)

  def update_work_unit(
      self, request: experiment_state_service_pb2.UpdateWorkUnitRequest
  ) -> work_unit_pb2.WorkUnit:
    return self._stub.UpdateWorkUnit(request)

  def create_artifact(
      self, request: experiment_state_service_pb2.CreateArtifactRequest
  ) -> artifact_pb2.Artifact:
    return self._stub.CreateArtifact(request)

  def list_artifacts(
      self, request: experiment_state_service_pb2.ListArtifactsRequest
  ) -> experiment_state_service_pb2.ListArtifactsResponse:
    return self._stub.ListArtifacts(request)

  def search_artifacts(
      self, request: experiment_state_service_pb2.ListArtifactsRequest
  ) -> experiment_state_service_pb2.ListArtifactsResponse:
    return self._stub.SearchArtifacts(request)

  def delete_artifact(
      self, request: experiment_state_service_pb2.DeleteArtifactRequest
  ) -> artifact_pb2.Artifact:
    return self._stub.DeleteArtifact(request)

  def update_artifact(
      self, request: experiment_state_service_pb2.UpdateArtifactRequest
  ) -> artifact_pb2.Artifact:
    return self._stub.UpdateArtifact(request)

  def list_status_messages(
      self, request: experiment_state_service_pb2.ListStatusMessagesRequest
  ) -> experiment_state_service_pb2.ListStatusMessagesResponse:
    return self._stub.ListStatusMessages(request)

  def search_status_messages(
      self, request: experiment_state_service_pb2.ListStatusMessagesRequest
  ) -> experiment_state_service_pb2.ListStatusMessagesResponse:
    return self._stub.SearchStatusMessages(request)

  def batch_restart_work_units(
      self, request: experiment_state_service_pb2.BatchRestartWorkUnitsRequest
  ) -> operations_pb2.Operation:
    return self._stub.BatchRestartWorkUnits(request)

  def batch_stop_work_units(
      self, request: experiment_state_service_pb2.BatchStopWorkUnitsRequest
  ) -> operations_pb2.Operation:
    return self._stub.BatchStopWorkUnits(request)


@functools.lru_cache(maxsize=1)
def get_experiment_state_api() -> ExperimentStateGRpcApi:
  """Returns a cached stub for the Experiment State Server."""

  return ExperimentStateGRpcApi()
