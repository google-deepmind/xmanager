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
"""Unit tests for experiment_state_api module."""

import base64
import json
import logging
import os
from typing import Any
import unittest
from unittest import mock

import google.auth
from google.oauth2 import id_token
import grpc

try:
  from google.longrunning import operations_pb2
except ImportError:
  from longrunning import operations_pb2

from xmanager_cloud.experiment_state_server import experiment_state_api
from xmanager_cloud.experiment_state_server.proto import (
    api_pb2 as experiment_state_service_pb2,
    artifact_pb2,
    experiment_pb2,
    work_unit_pb2,
)


class ExperimentStateApiTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self._mock_stub = mock.MagicMock()
    self._mock_stub.CreateExperiment.return_value = experiment_pb2.Experiment()
    self._mock_stub.CreateWorkUnit.return_value = operations_pb2.Operation()
    self._mock_stub.GetExperiment.return_value = experiment_pb2.Experiment()
    self._mock_stub.ListExperiments.return_value = (
        experiment_state_service_pb2.ListExperimentsResponse()
    )
    self._mock_stub.SearchExperiments.return_value = (
        experiment_state_service_pb2.ListExperimentsResponse()
    )
    self._mock_stub.GetWorkUnit.return_value = work_unit_pb2.WorkUnit()
    self._mock_stub.ListWorkUnits.return_value = (
        experiment_state_service_pb2.ListWorkUnitsResponse()
    )
    self._mock_stub.SearchWorkUnits.return_value = (
        experiment_state_service_pb2.ListWorkUnitsResponse()
    )
    self._mock_stub.UpdateExperimentLaunchState.return_value = (
        experiment_pb2.Experiment()
    )
    self._mock_stub.UpdateExperiment.return_value = experiment_pb2.Experiment()
    self._mock_stub.UpdateWorkUnit.return_value = work_unit_pb2.WorkUnit()
    self._mock_stub.CreateArtifact.return_value = artifact_pb2.Artifact()
    self._mock_stub.ListArtifacts.return_value = (
        experiment_state_service_pb2.ListArtifactsResponse()
    )
    self._mock_stub.SearchArtifacts.return_value = (
        experiment_state_service_pb2.ListArtifactsResponse()
    )
    self._mock_stub.DeleteArtifact.return_value = artifact_pb2.Artifact()
    self._mock_stub.UpdateArtifact.return_value = artifact_pb2.Artifact()
    self._mock_stub.ListStatusMessages.return_value = (
        experiment_state_service_pb2.ListStatusMessagesResponse()
    )
    self._mock_stub.SearchStatusMessages.return_value = (
        experiment_state_service_pb2.ListStatusMessagesResponse()
    )
    self._mock_stub.BatchRestartWorkUnits.return_value = (
        operations_pb2.Operation()
    )
    self._mock_stub.BatchStopWorkUnits.return_value = operations_pb2.Operation()
    self._mock_channel = mock.MagicMock()
    self.enterContext(
        mock.patch.object(
            experiment_state_api,
            '_create_experiment_state_server_stub',
            return_value=(self._mock_stub, self._mock_channel),
        )
    )

  def tearDown(self):
    super().tearDown()
    experiment_state_api.get_experiment_state_api.cache_clear()

  def test_get_experiment_state_api(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    experiment_state_api._create_experiment_state_server_stub.assert_called_once_with(
        experiment_state_api._XMANAGER_ENDPOINT,
    )
    self.assertEqual(ess_api._stub, self._mock_stub)

  def test_get_experiment_state_api_with_custom_endpoint(self):
    with mock.patch.dict(os.environ, {'XMANAGER_ENDPOINT': 'custom_endpoint'}):
      ess_api = experiment_state_api.get_experiment_state_api()
      experiment_state_api._create_experiment_state_server_stub.assert_called_once_with(
          'custom_endpoint',
      )
      self.assertEqual(ess_api._stub, self._mock_stub)

  def test_create_experiment(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.CreateExperimentRequest()
    ess_api.create_experiment(request)
    self._mock_stub.CreateExperiment.assert_called_once_with(request)

  def test_create_work_unit(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.CreateWorkUnitRequest()
    ess_api.create_work_unit(request)
    self._mock_stub.CreateWorkUnit.assert_called_once_with(request)

  def test_get_experiment(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.GetExperimentRequest()
    ess_api.get_experiment(request)
    self._mock_stub.GetExperiment.assert_called_once_with(request)

  def test_list_experiments(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListExperimentsRequest()
    ess_api.list_experiments(request)
    self._mock_stub.ListExperiments.assert_called_once_with(request)

  def test_search_experiments(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListExperimentsRequest()
    ess_api.search_experiments(request)
    self._mock_stub.SearchExperiments.assert_called_once_with(request)

  def test_get_work_unit(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.GetWorkUnitRequest()
    ess_api.get_work_unit(request)
    self._mock_stub.GetWorkUnit.assert_called_once_with(request)

  def test_list_work_units(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListWorkUnitsRequest()
    ess_api.list_work_units(request)
    self._mock_stub.ListWorkUnits.assert_called_once_with(request)

  def test_search_work_units(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListWorkUnitsRequest()
    ess_api.search_work_units(request)
    self._mock_stub.SearchWorkUnits.assert_called_once_with(request)

  def test_update_experiment_launch_state(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.UpdateExperimentLaunchStateRequest()
    ess_api.update_experiment_launch_state(request)
    self._mock_stub.UpdateExperimentLaunchState.assert_called_once_with(request)

  def test_update_experiment(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.UpdateExperimentRequest()
    ess_api.update_experiment(request)
    self._mock_stub.UpdateExperiment.assert_called_once_with(request)

  def test_update_work_unit(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.UpdateWorkUnitRequest()
    ess_api.update_work_unit(request)
    self._mock_stub.UpdateWorkUnit.assert_called_once_with(request)

  def test_create_artifact(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.CreateArtifactRequest()
    ess_api.create_artifact(request)
    self._mock_stub.CreateArtifact.assert_called_once_with(request)

  def test_list_artifacts(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListArtifactsRequest()
    ess_api.list_artifacts(request)
    self._mock_stub.ListArtifacts.assert_called_once_with(request)

  def test_search_artifacts(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListArtifactsRequest()
    ess_api.search_artifacts(request)
    self._mock_stub.SearchArtifacts.assert_called_once_with(request)

  def test_delete_artifact(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.DeleteArtifactRequest()
    ess_api.delete_artifact(request)
    self._mock_stub.DeleteArtifact.assert_called_once_with(request)

  def test_update_artifact(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.UpdateArtifactRequest()
    ess_api.update_artifact(request)
    self._mock_stub.UpdateArtifact.assert_called_once_with(request)

  def test_list_status_messages(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListStatusMessagesRequest()
    ess_api.list_status_messages(request)
    self._mock_stub.ListStatusMessages.assert_called_once_with(request)

  def test_search_status_messages(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.ListStatusMessagesRequest()
    ess_api.search_status_messages(request)
    self._mock_stub.SearchStatusMessages.assert_called_once_with(request)

  def test_batch_restart_work_units(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.BatchRestartWorkUnitsRequest()
    ess_api.batch_restart_work_units(request)
    self._mock_stub.BatchRestartWorkUnits.assert_called_once_with(request)

  def test_batch_stop_work_units(self):
    ess_api = experiment_state_api.get_experiment_state_api()
    request = experiment_state_service_pb2.BatchStopWorkUnitsRequest()
    ess_api.batch_stop_work_units(request)
    self._mock_stub.BatchStopWorkUnits.assert_called_once_with(request)


class ExtractEmailFromJwtTest(unittest.TestCase):

  def _make_jwt(self, payload_dict_or_obj: Any) -> str:
    header = (
        base64.urlsafe_b64encode(b'{"alg":"none"}')
        .decode('utf-8')
        .rstrip('=')
    )
    if isinstance(payload_dict_or_obj, (dict, list, int, str)):
      payload_json = json.dumps(payload_dict_or_obj).encode('utf-8')
    else:
      payload_json = payload_dict_or_obj
    payload = (
        base64.urlsafe_b64encode(payload_json).decode('utf-8').rstrip('=')
    )
    sig = (
        base64.urlsafe_b64encode(b'signature')
        .decode('utf-8')
        .rstrip('=')
    )
    return f'{header}.{payload}.{sig}'

  def test_valid_jwt_with_email_claim(self):
    token = self._make_jwt({'email': 'alice@example.com'})
    self.assertEqual(
        experiment_state_api._extract_email_from_jwt(token), 'alice@example.com'
    )

  def test_valid_jwt_with_preferred_username_claim(self):
    token = self._make_jwt({'preferred_username': 'alice_user'})
    self.assertEqual(
        experiment_state_api._extract_email_from_jwt(token), 'alice_user'
    )

  def test_valid_jwt_with_sub_claim(self):
    token = self._make_jwt({'sub': 'user_sub_123'})
    self.assertEqual(
        experiment_state_api._extract_email_from_jwt(token), 'user_sub_123'
    )

  def test_claim_precedence_email_over_others(self):
    token = self._make_jwt({
        'email': 'alice@example.com',
        'preferred_username': 'alice_user',
        'sub': 'user_sub_123',
    })
    self.assertEqual(
        experiment_state_api._extract_email_from_jwt(token), 'alice@example.com'
    )

  def test_claim_precedence_preferred_username_over_sub(self):
    token = self._make_jwt({
        'preferred_username': 'alice_user',
        'sub': 'user_sub_123',
    })
    self.assertEqual(
        experiment_state_api._extract_email_from_jwt(token), 'alice_user'
    )

  def test_invalid_jwt_segments(self):
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(''))
    self.assertIsNone(experiment_state_api._extract_email_from_jwt('one_segment'))
    self.assertIsNone(experiment_state_api._extract_email_from_jwt('one.two'))
    self.assertIsNone(
        experiment_state_api._extract_email_from_jwt('one.two.three.four')
    )

  def test_bad_base64_payload_and_padding(self):
    self.assertIsNone(
        experiment_state_api._extract_email_from_jwt('head.???bad_b64???.sig')
    )

  def test_non_json_payload(self):
    raw_payload = base64.urlsafe_b64encode(b'this is not valid json').decode(
        'utf-8'
    )
    token = f'head.{raw_payload}.sig'
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(token))

  def test_non_dict_payload(self):
    token_list = self._make_jwt(['not', 'a', 'dict'])
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(token_list))
    token_int = self._make_jwt(12345)
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(token_int))
    token_str = self._make_jwt('just a string')
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(token_str))

  def test_no_matching_claims(self):
    token = self._make_jwt({'aud': 'audience', 'iat': 1234567890})
    self.assertIsNone(experiment_state_api._extract_email_from_jwt(token))


class GetCurrentUserEmailTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_default = self.enterContext(
        mock.patch.object(google.auth, 'default', autospec=True)
    )
    self.mock_creds = mock.MagicMock()
    self.mock_default.return_value = (self.mock_creds, None)
    self.mock_creds.valid = True
    del self.mock_creds.service_account_email
    self.mock_creds.id_token = None

    self.mock_verify_oauth2_token = self.enterContext(
        mock.patch.object(id_token, 'verify_oauth2_token', autospec=True)
    )
    self.mock_authorized_session = self.enterContext(
        mock.patch(
            'google.auth.transport.requests.AuthorizedSession', autospec=True
        )
    )

  def test_user_email_env_override(self):
    with mock.patch.dict(os.environ, {'XMC_USER_EMAIL': 'custom@example.com'}):
      self.assertEqual(
          experiment_state_api.get_current_user_email(), 'custom@example.com'
      )

  def test_opaque_auth_token_without_claims_falls_through_to_google_auth(self):
    with mock.patch.dict(
        os.environ, {'XMC_AUTH_TOKEN': 'opaque_token'}, clear=True
    ):
      mock_creds = mock.MagicMock(service_account_email='sa@google.com')
      with mock.patch.object(
          google.auth, 'default', return_value=(mock_creds, 'project')
      ):
        self.assertEqual(
            experiment_state_api.get_current_user_email(), 'sa@google.com'
        )

  def test_user_email_with_jwt_auth_token_extracts_email(self):
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode('utf-8')
    payload = base64.urlsafe_b64encode(
        b'{"email":"jwt_user@example.com"}'
    ).decode('utf-8')
    token = f'{header}.{payload}.sig'
    with mock.patch.dict(os.environ, {'XMC_AUTH_TOKEN': token}, clear=True):
      self.assertEqual(
          experiment_state_api.get_current_user_email(), 'jwt_user@example.com'
      )

  def test_user_email_override_takes_precedence_over_auth_token(self):
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').decode('utf-8')
    payload = base64.urlsafe_b64encode(
        b'{"email":"jwt_user@example.com"}'
    ).decode('utf-8')
    token = f'{header}.{payload}.sig'
    with mock.patch.dict(
        os.environ,
        {'XMC_USER_EMAIL': 'user@example.com', 'XMC_AUTH_TOKEN': token},
    ):
      self.assertEqual(
          experiment_state_api.get_current_user_email(), 'user@example.com'
      )

  def test_service_account_email(self):
    self.mock_creds.service_account_email = 'sa@example.com'
    self.assertEqual(
        experiment_state_api.get_current_user_email(), 'sa@example.com'
    )

  def test_id_token_email(self):
    self.mock_creds.id_token = 'some_token'
    self.mock_verify_oauth2_token.return_value = {'email': 'id@example.com'}
    self.assertEqual(
        experiment_state_api.get_current_user_email(), 'id@example.com'
    )

  def test_userinfo_email_after_id_token_value_error(self):
    self.mock_creds.id_token = 'some_token'
    self.mock_verify_oauth2_token.side_effect = ValueError('Invalid token')
    mock_session = (
        self.mock_authorized_session.return_value.__enter__.return_value
    )
    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'email': 'userinfo@example.com'}
    mock_session.get.return_value = mock_response

    self.assertEqual(
        experiment_state_api.get_current_user_email(), 'userinfo@example.com'
    )
    mock_session.get.assert_called_once_with(
        'https://openidconnect.googleapis.com/v1/userinfo'
    )

  def test_userinfo_email_no_id_token(self):
    mock_session = (
        self.mock_authorized_session.return_value.__enter__.return_value
    )
    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'email': 'userinfo@example.com'}
    mock_session.get.return_value = mock_response

    self.assertEqual(
        experiment_state_api.get_current_user_email(), 'userinfo@example.com'
    )
    mock_session.get.assert_called_once_with(
        'https://openidconnect.googleapis.com/v1/userinfo'
    )

  def test_userinfo_not_200(self):
    mock_session = (
        self.mock_authorized_session.return_value.__enter__.return_value
    )
    mock_response = mock.MagicMock()
    mock_response.status_code = 403
    mock_session.get.return_value = mock_response

    with self.assertRaisesRegex(
        RuntimeError, 'Failed to get current user email'
    ):
      experiment_state_api.get_current_user_email()

  def test_userinfo_no_email(self):
    mock_session = (
        self.mock_authorized_session.return_value.__enter__.return_value
    )
    mock_response = mock.MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}
    mock_session.get.return_value = mock_response

    with self.assertRaisesRegex(
        RuntimeError, 'Failed to get current user email'
    ):
      experiment_state_api.get_current_user_email()

  def test_auth_default_exception(self):
    self.mock_default.side_effect = Exception('Auth failed')
    with self.assertRaisesRegex(
        RuntimeError, 'Failed to get current user email'
    ):
      experiment_state_api.get_current_user_email()


class InterceptorsTest(unittest.TestCase):

  def test_audit_metadata_interceptor(self):
    interceptor = experiment_state_api.AuditMetadataInterceptor(
        user_email='test@example.com'
    )
    mock_call_details = mock.MagicMock()
    mock_call_details.metadata = [('initial-key', 'initial-value')]
    mock_call_details._replace = mock.MagicMock(return_value='replaced_details')
    mock_continuation = mock.MagicMock(return_value='response')

    result = interceptor.intercept_unary_unary(
        mock_continuation, mock_call_details, 'request'
    )

    self.assertEqual(result, 'response')
    mock_call_details._replace.assert_called_once_with(
        metadata=[
            ('initial-key', 'initial-value'),
            ('x-goog-user-email', 'test@example.com'),
        ]
    )
    mock_continuation.assert_called_once_with('replaced_details', 'request')

  def test_bearer_auth_interceptor(self):
    interceptor = experiment_state_api.BearerAuthInterceptor(
        token='mock_bearer_token', user_email='test@example.com'
    )
    mock_call_details = mock.MagicMock()
    mock_call_details.metadata = []
    mock_call_details._replace = mock.MagicMock(return_value='replaced_details')
    mock_continuation = mock.MagicMock(return_value='response')

    result = interceptor.intercept_unary_unary(
        mock_continuation, mock_call_details, 'request'
    )

    self.assertEqual(result, 'response')
    mock_call_details._replace.assert_called_once_with(
        metadata=[
            ('authorization', 'Bearer mock_bearer_token'),
            ('x-goog-user-email', 'test@example.com'),
        ]
    )
    mock_continuation.assert_called_once_with('replaced_details', 'request')


class CreateStubTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.mock_channel_ready_future = self.enterContext(
        mock.patch.object(grpc, 'channel_ready_future', autospec=True)
    )
    self.mock_future = mock.MagicMock()
    self.mock_channel_ready_future.return_value = self.mock_future
    self.mock_future.result.return_value = None

  def test_secure_channel_succeeds_with_auth_token(self):
    with (
        mock.patch.dict(
            os.environ,
            {
                'XMC_AUTH_TOKEN': 'token_abc',
                'XMC_USER_EMAIL': 'user@example.com',
            },
            clear=True,
        ),
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'insecure_channel') as mock_insecure_channel,
        mock.patch.object(
            grpc, 'access_token_call_credentials'
        ) as mock_access_token_creds,
        mock.patch.object(
            grpc, 'ssl_channel_credentials'
        ) as mock_ssl_channel_creds,
        mock.patch.object(
            grpc, 'composite_channel_credentials'
        ) as mock_composite_creds,
        mock.patch.object(grpc, 'intercept_channel') as mock_intercept_channel,
    ):
      mock_channel = mock.MagicMock()
      mock_secure_channel.return_value = mock_channel
      mock_intercept_channel.return_value = mock_channel

      stub, channel = experiment_state_api._create_experiment_state_server_stub(
          'dns:///api.example.com'
      )
      mock_access_token_creds.assert_called_once_with('token_abc')
      mock_secure_channel.assert_called_once()
      mock_insecure_channel.assert_not_called()
      interceptor = mock_intercept_channel.call_args[0][1]
      self.assertIsInstance(
          interceptor, experiment_state_api.AuditMetadataInterceptor
      )
      self.assertEqual(interceptor._user_email, 'user@example.com')
      self.mock_channel_ready_future.assert_called_once_with(mock_channel)
      self.mock_future.result.assert_called_once_with(
          experiment_state_api._CHANNEL_READY_TIMEOUT_SEC
      )

  def test_secure_channel_succeeds_even_when_insecure_flag_set(self):
    with (
        mock.patch.dict(
            os.environ,
            {
                'XMC_AUTH_TOKEN': 'token_abc',
                'XMC_INSECURE_GRPC': 'true',
                'XMC_USER_EMAIL': 'user@example.com',
            },
            clear=True,
        ),
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'insecure_channel') as mock_insecure_channel,
        mock.patch.object(grpc, 'access_token_call_credentials'),
        mock.patch.object(grpc, 'ssl_channel_credentials'),
        mock.patch.object(grpc, 'composite_channel_credentials'),
        mock.patch.object(grpc, 'intercept_channel') as mock_intercept_channel,
    ):
      mock_channel = mock.MagicMock()
      mock_secure_channel.return_value = mock_channel
      mock_intercept_channel.return_value = mock_channel

      stub, channel = experiment_state_api._create_experiment_state_server_stub(
          'dns:///api.example.com'
      )
      mock_secure_channel.assert_called_once()
      mock_insecure_channel.assert_not_called()

  def test_secure_channel_fails_and_insecure_allowed_falls_back_to_insecure(self):
    with (
        mock.patch.dict(
            os.environ,
            {
                'XMC_AUTH_TOKEN': 'token_abc',
                'XMANAGER_INSECURE_GRPC': 'true',
                'XMC_USER_EMAIL': 'user@example.com',
            },
            clear=True,
        ),
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'insecure_channel') as mock_insecure_channel,
        mock.patch.object(grpc, 'access_token_call_credentials'),
        mock.patch.object(grpc, 'ssl_channel_credentials'),
        mock.patch.object(grpc, 'composite_channel_credentials'),
        mock.patch.object(grpc, 'intercept_channel') as mock_intercept_channel,
        mock.patch('logging.warning') as mock_warning,
    ):
      mock_sec_channel = mock.MagicMock()
      mock_insec_channel = mock.MagicMock()
      mock_secure_channel.return_value = mock_sec_channel
      mock_insecure_channel.return_value = mock_insec_channel
      mock_intercept_channel.side_effect = [mock_sec_channel, mock_insec_channel]

      self.mock_future.result.side_effect = [
          Exception('TLS handshake timeout'),
          None,
      ]

      stub, channel = experiment_state_api._create_experiment_state_server_stub(
          'localhost:50051'
      )
      mock_secure_channel.assert_called_once()
      mock_insecure_channel.assert_called_once_with('localhost:50051')
      mock_warning.assert_called_once()
      self.assertIn('Falling back to insecure', mock_warning.call_args[0][0])
      self.assertEqual(channel, mock_insec_channel)

  def test_secure_channel_fails_and_insecure_with_xmc_insecure_grpc_flag(self):
    with (
        mock.patch.dict(
            os.environ,
            {
                'XMC_AUTH_TOKEN': 'token_abc',
                'XMC_INSECURE_GRPC': '1',
                'XMC_USER_EMAIL': 'user@example.com',
            },
            clear=True,
        ),
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'insecure_channel') as mock_insecure_channel,
        mock.patch.object(grpc, 'access_token_call_credentials'),
        mock.patch.object(grpc, 'ssl_channel_credentials'),
        mock.patch.object(grpc, 'composite_channel_credentials'),
        mock.patch.object(grpc, 'intercept_channel') as mock_intercept_channel,
        mock.patch('logging.warning') as mock_warning,
    ):
      mock_sec_channel = mock.MagicMock()
      mock_insec_channel = mock.MagicMock()
      mock_secure_channel.return_value = mock_sec_channel
      mock_insecure_channel.return_value = mock_insec_channel
      mock_intercept_channel.side_effect = [mock_sec_channel, mock_insec_channel]

      self.mock_future.result.side_effect = [
          Exception('TLS connection refused'),
          None,
      ]

      stub, channel = experiment_state_api._create_experiment_state_server_stub(
          'localhost:50051'
      )
      mock_secure_channel.assert_called_once()
      mock_insecure_channel.assert_called_once_with('localhost:50051')
      mock_warning.assert_called_once()
      self.assertIn('Falling back to insecure', mock_warning.call_args[0][0])
      interceptor = mock_intercept_channel.call_args[0][1]
      self.assertIsInstance(
          interceptor, experiment_state_api.BearerAuthInterceptor
      )
      self.assertEqual(interceptor._user_email, 'user@example.com')

  def test_secure_channel_fails_and_insecure_not_allowed_raises_runtime_error(self):
    with (
        mock.patch.dict(
            os.environ,
            {
                'XMC_AUTH_TOKEN': 'token_abc',
                'XMC_USER_EMAIL': 'user@example.com',
            },
            clear=True,
        ),
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'insecure_channel') as mock_insecure_channel,
        mock.patch.object(grpc, 'access_token_call_credentials'),
        mock.patch.object(grpc, 'ssl_channel_credentials'),
        mock.patch.object(grpc, 'composite_channel_credentials'),
        mock.patch.object(grpc, 'intercept_channel') as mock_intercept_channel,
    ):
      mock_channel = mock.MagicMock()
      mock_secure_channel.return_value = mock_channel
      mock_intercept_channel.return_value = mock_channel
      self.mock_future.result.side_effect = Exception('TLS connection refused')

      with self.assertRaisesRegex(
          RuntimeError,
          r'Failed to establish secure gRPC connection to localhost:50051',
      ):
        experiment_state_api._create_experiment_state_server_stub(
            'localhost:50051'
        )

      mock_secure_channel.assert_called_once()
      mock_insecure_channel.assert_not_called()

  def test_iap_and_sa_prompts_use_logging_warning_instead_of_print(self):
    with (
        mock.patch.dict(
            os.environ,
            {'XMC_USER_EMAIL': 'user@example.com'},
            clear=True,
        ),
        mock.patch(
            'builtins.input', side_effect=['test_iap_id', 'sa@example.com']
        ),
        mock.patch('builtins.print') as mock_print,
        mock.patch('logging.warning') as mock_warning,
        mock.patch.object(
            google.auth, 'default', return_value=(mock.MagicMock(), None)
        ),
        mock.patch(
            'google.auth.transport.requests.AuthorizedSession'
        ) as mock_auth_session,
        mock.patch.object(grpc, 'secure_channel') as mock_secure_channel,
        mock.patch.object(grpc, 'access_token_call_credentials'),
        mock.patch.object(grpc, 'ssl_channel_credentials'),
        mock.patch.object(grpc, 'composite_channel_credentials'),
        mock.patch.object(grpc, 'intercept_channel'),
    ):
      mock_session = mock_auth_session.return_value
      mock_response = mock.MagicMock()
      mock_response.status_code = 200
      mock_response.json.return_value = {'token': 'mock_oidc_token'}
      mock_session.post.return_value = mock_response

      experiment_state_api._create_experiment_state_server_stub(
          'dns:///api.example.com'
      )

      mock_print.assert_not_called()
      self.assertEqual(mock_warning.call_count, 2)
      self.assertIn(
          'IAP_CLIENT_ID not set', mock_warning.call_args_list[0][0][0]
      )
      self.assertIn(
          'XMC_CLIENT_SA not set', mock_warning.call_args_list[1][0][0]
      )


if __name__ == '__main__':
  unittest.main()
