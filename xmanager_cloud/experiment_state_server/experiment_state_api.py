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
import json
import logging
import math
import os
import pathlib
import stat
import threading
import time
from typing import Any, Callable
import urllib.parse

import google.auth
from google.auth import jwt as google_jwt
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
_CHANNEL_READY_TIMEOUT_SEC = 5.0
# Google Cloud IAM OIDC tokens have a strict maximum lifetime of 3600 seconds
# (60 minutes). We cache and refresh at 3300 seconds (55 minutes) to provide a
# 5-minute safety buffer against network latency, clock skew, and in-flight RPCs
# before the server or Cloud IAP rejects the token as expired.
_TOKEN_TTL_SEC = 3300.0
_CLI_TOKEN_EXPIRY_BUFFER_SEC = 60.0


def _get_xmanager_endpoint() -> str:
  if env_var := os.environ.get('XMANAGER_ENDPOINT'):
    return env_var
  return _XMANAGER_ENDPOINT


def _get_credentials_file_path() -> pathlib.Path:
  """Returns the path to the CLI credentials cache file."""
  if xdg := os.environ.get('XDG_CONFIG_HOME', '').strip():
    if os.path.isabs(xdg):
      return pathlib.Path(xdg) / 'xmc' / 'credentials.json'
  return pathlib.Path.home() / '.config' / 'xmc' / 'credentials.json'


def _extract_grpc_host(endpoint: str) -> str:
  """Extracts the lowercased hostname from a gRPC target string."""
  target = endpoint.strip().lower().removeprefix('dns:///')
  try:
    return urllib.parse.urlsplit(f'//{target}').hostname or ''
  except ValueError:
    return ''


def _resolve_xmc_origin(endpoint: str) -> str | None:
  """Resolves the XMC deployment origin URL for `endpoint`."""
  grpc_host = _extract_grpc_host(endpoint)
  if grpc_host in ('localhost', '127.0.0.1', '::1'):
    raw_url = os.environ.get('XMC_API_URL') or os.environ.get('XMANAGER_UI_URL')
    parts = urllib.parse.urlsplit((raw_url or '').strip())
    if parts.scheme.lower() not in ('http', 'https') or not parts.hostname:
      return None
    return f'{parts.scheme.lower()}://{parts.netloc.lower()}'
  if grpc_host.startswith('grpc.') and endpoint != _XMANAGER_ENDPOINT:
    domain = grpc_host.removeprefix('grpc.')
    return f'https://{domain}' if domain else None
  return None


def _read_credentials_deployments(path: pathlib.Path) -> dict[str, Any]:
  """Reads the `deployments` map from a `0600` credentials file."""
  try:
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
  except OSError:
    return {}
  with os.fdopen(fd, 'rb') as f:
    st = os.fstat(f.fileno())
    if not stat.S_ISREG(st.st_mode) or (st.st_mode & 0o077) != 0:
      logging.warning(
          'Ignoring %s: unsafe file mode %s (expected 0600).',
          path,
          oct(stat.S_IMODE(st.st_mode)),
      )
      return {}
    try:
      data = json.load(f)
    except ValueError:
      return {}
  if not isinstance(data, dict) or data.get('version') != 1:
    return {}
  deployments = data.get('deployments')
  return deployments if isinstance(deployments, dict) else {}


def _load_cached_cli_token(endpoint: str | None = None) -> str | None:
  """Returns a valid cached CLI bearer token for `endpoint`, if available."""
  origin = _resolve_xmc_origin(endpoint or _get_xmanager_endpoint())
  if not origin:
    return None
  entry = _read_credentials_deployments(_get_credentials_file_path()).get(
      origin
  )
  if not isinstance(entry, dict):
    return None
  token = entry.get('access_token')
  expires_at = entry.get('expires_at')
  if not isinstance(token, str) or not token.strip():
    return None
  if (
      isinstance(expires_at, bool)
      or not isinstance(expires_at, (int, float))
      or not math.isfinite(expires_at)
  ):
    return None
  if expires_at <= time.time() + _CLI_TOKEN_EXPIRY_BUFFER_SEC:
    logging.warning(
        'Ignoring expired XMC CLI token for %s. Run `xmc login --server %s`.',
        origin,
        origin,
    )
    return None
  return token.strip()


def _extract_email_from_jwt(token: str) -> str | None:
  """Extracts user identity claim from an unverified JWT using google-auth."""
  try:
    payload = google_jwt.decode(token, verify=False)
    if not isinstance(payload, dict):
      return None
    for claim in ('email', 'preferred_username', 'sub'):
      if val := payload.get(claim):
        return str(val)
  except Exception:  # pylint: disable=broad-exception-caught
    return None
  return None


def get_current_user_email() -> str:
  """Retrieves the email of the current authenticated user.

  This function attempts to get the user's email from the credentials,
  prioritizing explicit environment variables, then auth token claims,
  service account email, ID token claims, and finally
  the OpenID Connect userinfo endpoint.

  Returns:
    The email address of the current user.

  Raises:
    RuntimeError: If the user email cannot be retrieved.
  """
  if user_email := os.environ.get('XMC_USER_EMAIL'):
    return user_email

  if auth_token := os.environ.get('XMC_AUTH_TOKEN'):
    if extracted_email := _extract_email_from_jwt(auth_token):
      return extracted_email
    logging.warning(
        'XMC_AUTH_TOKEN is set but no identity claim could be extracted from'
        ' it.'
    )
  elif cached_token := _load_cached_cli_token():
    if extracted_email := _extract_email_from_jwt(cached_token):
      return extracted_email

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


class IamIdTokenProvider:
  """Token provider that fetches and caches Google Cloud IAM OIDC tokens."""

  def __init__(self, client_sa: str, audience: str):
    self._client_sa = client_sa
    self._audience = audience
    self._cached_token: str | None = None
    self._expiry_time: float = 0.0
    self._lock = threading.Lock()

  def _fetch_token(self) -> str:
    """Fetches a fresh OIDC identity token from Google Cloud IAM."""
    try:
      credentials, _ = google.auth.default()
      authed_session = google.auth.transport.requests.AuthorizedSession(
          credentials
      )
      url = (
          'https://iamcredentials.googleapis.com/v1/projects/-/'
          f'serviceAccounts/{self._client_sa}:generateIdToken'
      )
      body = {'audience': self._audience, 'includeEmail': True}
      response = authed_session.post(url, json=body)
      response.raise_for_status()
      return response.json()['token']
    except Exception as e:
      raise RuntimeError('Failed to get identity token via google-auth') from e

  def __call__(self) -> str:
    """Returns a valid ID token, refreshing if expired or expiring soon."""
    with self._lock:
      now = time.time()
      if self._cached_token is None or now >= self._expiry_time:
        self._cached_token = self._fetch_token()
        self._expiry_time = now + _TOKEN_TTL_SEC
      return self._cached_token


class AuthMetadataPlugin(grpc.AuthMetadataPlugin):
  """gRPC AuthMetadataPlugin to dynamically attach fresh bearer auth tokens."""

  def __init__(self, token_provider: Callable[[], str]):
    super().__init__()
    self._token_provider = token_provider

  def __call__(
      self,
      context: grpc.AuthMetadataContext,
      callback: grpc.AuthMetadataPluginCallback,
  ) -> None:
    try:
      token = self._token_provider()
      callback([('authorization', f'Bearer {token}')], None)
    except Exception as e:  # pylint: disable=broad-exception-caught
      callback(None, e)


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
    if self._user_email:
      metadata.append(('x-goog-user-email', self._user_email))

    return continuation(
        client_call_details._replace(metadata=metadata), request
    )


class BearerAuthInterceptor(grpc.UnaryUnaryClientInterceptor):
  """gRPC client interceptor to inject Authorization Bearer token and user email metadata."""

  def __init__(
      self,
      token: str | Callable[[], str],
      user_email: str = '',
  ):
    self._token_provider = token if callable(token) else (lambda: token)
    self._user_email = user_email

  def intercept_unary_unary(
      self,
      continuation: Callable[[Any, Any], Any],
      client_call_details: Any,
      request: Any,
  ) -> Any:
    metadata = list(client_call_details.metadata or [])
    if token := self._token_provider():
      metadata.append(('authorization', f'Bearer {token}'))
    if self._user_email:
      metadata.append(('x-goog-user-email', self._user_email))

    return continuation(
        client_call_details._replace(metadata=metadata), request
    )


def _build_secure_channel(
    endpoint: str,
    token_provider: Callable[[], str],
    user_email: str,
) -> grpc.Channel:
  """Builds a secure TLS gRPC channel with authentication and audit interceptors."""
  channel_creds = grpc.ssl_channel_credentials()
  call_creds = grpc.metadata_call_credentials(
      AuthMetadataPlugin(token_provider)
  )
  composite_creds = grpc.composite_channel_credentials(
      channel_creds, call_creds
  )
  channel = grpc.secure_channel(endpoint, composite_creds)
  return grpc.intercept_channel(channel, AuditMetadataInterceptor(user_email))


def _build_insecure_channel(
    endpoint: str,
    token_provider: Callable[[], str],
    user_email: str,
) -> grpc.Channel:
  """Builds an insecure gRPC channel with authentication and audit interceptors."""
  channel = grpc.insecure_channel(endpoint)
  interceptor = BearerAuthInterceptor(
      token=token_provider, user_email=user_email
  )
  return grpc.intercept_channel(channel, interceptor)


def _create_experiment_state_server_stub(
    endpoint: str,
) -> tuple[
    experiment_state_service_pb2_grpc.ExperimentStateServiceStub,
    grpc.Channel,
]:
  """Create a gRPC stub and channel for the Experiment State Server."""
  use_http = False
  if endpoint.startswith('http://'):
    endpoint = endpoint[len('http://') :]
    use_http = True
  elif endpoint.startswith('https://'):
    endpoint = endpoint[len('https://') :]

  insecure_allowed = (
      use_http
      or os.environ.get('XMANAGER_INSECURE_GRPC', '').lower()
      in ('true', '1', 'yes')
      or os.environ.get('XMC_INSECURE_GRPC', '').lower()
      in (
          'true',
          '1',
          'yes',
      )
  )
  auth_token = os.environ.get('XMC_AUTH_TOKEN') or _load_cached_cli_token(
      endpoint
  )
  user_email = (
      os.environ.get('XMC_USER_EMAIL')
      or (auth_token and _extract_email_from_jwt(auth_token))
      or get_current_user_email()
  )

  token_provider: Callable[[], str]
  if auth_token:
    token_provider = lambda: auth_token
  elif use_http:
    token_provider = lambda: ''
  else:
    iap_client_id = os.environ.get('IAP_CLIENT_ID')
    if not iap_client_id:
      logging.warning(
          'IAP_CLIENT_ID not set. It should be the OAuth client ID used by IAP '
          'for your backend.'
      )
      iap_client_id = input('Enter IAP_CLIENT_ID: ')
    client_sa = os.environ.get('XMC_CLIENT_SA')
    if not client_sa:
      logging.warning(
          'XMC_CLIENT_SA not set. It should be the email of the service'
          ' account to impersonate (e.g.,'
          ' xmc-client-username@my-project-id.iam.gserviceaccount.com).'
      )
      client_sa = input('Enter XMC_CLIENT_SA: ')

    token_provider = IamIdTokenProvider(
        client_sa=client_sa,
        audience=iap_client_id,
    )

  if use_http:
    intercepted_channel = _build_insecure_channel(
        endpoint=endpoint,
        token_provider=token_provider,
        user_email=user_email,
    )
    grpc.channel_ready_future(intercepted_channel).result(
        _CHANNEL_READY_TIMEOUT_SEC
    )
  else:
    try:
      intercepted_channel = _build_secure_channel(
          endpoint=endpoint,
          token_provider=token_provider,
          user_email=user_email,
      )
      grpc.channel_ready_future(intercepted_channel).result(
          _CHANNEL_READY_TIMEOUT_SEC
      )
    except Exception as e:  # pylint: disable=broad-exception-caught
      if not insecure_allowed:
        raise RuntimeError(
            f'Failed to establish secure gRPC connection to {endpoint}'
        ) from e
      logging.warning(
          'Secure gRPC connection to %s failed (%s). Falling back to insecure'
          ' channel as requested by configuration.',
          endpoint,
          e,
      )
      intercepted_channel = _build_insecure_channel(
          endpoint=endpoint,
          token_provider=token_provider,
          user_email=user_email,
      )
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
  """Convenience wrapper for the Experiment State Server gRPC API."""

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
