# Copyright 2026 Google LLC
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

import os
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized

# 1. Import artifact_pb2 and api_pb2 first so we can check
# if they have the definitions
from xmanager_cloud.experiment_state_server.proto import api_pb2 as experiment_state_service_pb2
from xmanager_cloud.experiment_state_server.proto import artifact_pb2

# Suppress git executable warning/errors in sandboxed test environments
os.environ['GIT_PYTHON_REFRESH'] = 'quiet'

# Check if TextArtifact is already in the compiled protobuf library.
# If it is missing, we apply mocking.
is_mocked = not hasattr(artifact_pb2, 'TextArtifact')

if is_mocked:
  # Overwrite Artifact, ArtifactPayload, and Request classes with Mocks
  artifact_pb2.Artifact = mock.MagicMock(name='Artifact')
  artifact_pb2.Artifact.AdditionalInfo = mock.MagicMock(name='AdditionalInfo')
  artifact_pb2.ArtifactPayload = mock.MagicMock(name='ArtifactPayload')
  experiment_state_service_pb2.CreateArtifactRequest = mock.MagicMock(
      name='CreateArtifactRequest'
  )

  # Specs to enforce attribute presence for mock instances
  class SpecTextArtifact:
    text = ''
    title = ''

  class SpecUrlArtifact:
    url = ''
    display_name = ''
    icon = ''

  class SpecCodeSourceArtifact:
    code_block = ''
    code_url = ''
    title = ''

  # Define mock classes with __name__ and spec-based return values
  mock_text_artifact = mock.MagicMock(name='TextArtifact')
  mock_text_artifact.__name__ = 'TextArtifact'
  mock_text_artifact.return_value = mock.MagicMock(
      name='TextArtifactInstance', spec=SpecTextArtifact
  )
  artifact_pb2.TextArtifact = mock_text_artifact

  mock_url_artifact = mock.MagicMock(name='UrlArtifact')
  mock_url_artifact.__name__ = 'UrlArtifact'
  mock_url_artifact.return_value = mock.MagicMock(
      name='UrlArtifactInstance', spec=SpecUrlArtifact
  )
  artifact_pb2.UrlArtifact = mock_url_artifact

  mock_code_artifact = mock.MagicMock(name='CodeSourceArtifact')
  mock_code_artifact.__name__ = 'CodeSourceArtifact'
  mock_code_artifact.return_value = mock.MagicMock(
      name='CodeSourceArtifactInstance', spec=SpecCodeSourceArtifact
  )
  artifact_pb2.CodeSourceArtifact = mock_code_artifact

  # Setup Mock descriptor on ArtifactPayload
  class MockMessageType:

    def __init__(self, name, fields):
      self.name = name
      self.fields = [mock.MagicMock(name=f_name) for f_name in fields]
      for f, f_name in zip(self.fields, fields):
        f.name = f_name

  class MockField:

    def __init__(self, name, message_name, message_fields):
      self.name = name
      self.message_type = MockMessageType(message_name, message_fields)

  mock_oneof = mock.MagicMock()
  mock_oneof.fields = [
      MockField('text', 'TextArtifact', ['text', 'title']),
      MockField('url', 'UrlArtifact', ['url', 'display_name', 'icon']),
      MockField(
          'code_source',
          'CodeSourceArtifact',
          ['code_block', 'code_url', 'title'],
      ),
      MockField(
          'managed_xprof', 'ManagedXprofArtifact', ['display_name', 'run_group']
      ),
  ]

  mock_desc = mock.MagicMock()
  mock_desc.oneofs_by_name = {'type': mock_oneof}
  artifact_pb2.ArtifactPayload.DESCRIPTOR = mock_desc


# According to the Python style guide, third-party imports (like 'xmanager')
# should typically come before and previous other imports. However, in this
# test, we need to mock classes within 'experiment'
# *before* the 'xmanager.xm_cloud.experiment' module is imported, as
# 'experiment' depends on them. The mocking logic must execute before
# 'experiment' is loaded. Thus, we temporarily disable the import order check.
# pylint: disable=g-import-not-at-top,g-bad-import-order
from xmanager.xm_cloud import experiment

# pylint: enable=g-import-not-at-top,g-bad-import-order


class ProcessArtifactPayloadTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    if is_mocked:
      # Reset call histories of the mock classes
      artifact_pb2.Artifact.AdditionalInfo.reset_mock()
      artifact_pb2.ArtifactPayload.reset_mock()
      artifact_pb2.TextArtifact.reset_mock()
      artifact_pb2.UrlArtifact.reset_mock()
      artifact_pb2.CodeSourceArtifact.reset_mock()

      # Configure mock return values default attributes to avoid unexpected
      # truthy values unless set by the test code.
      artifact_pb2.TextArtifact.return_value.text = ''
      artifact_pb2.TextArtifact.return_value.title = ''
      artifact_pb2.UrlArtifact.return_value.url = ''
      artifact_pb2.UrlArtifact.return_value.display_name = ''
      artifact_pb2.UrlArtifact.return_value.icon = ''
      artifact_pb2.CodeSourceArtifact.return_value.code_block = ''
      artifact_pb2.CodeSourceArtifact.return_value.code_url = ''
      artifact_pb2.CodeSourceArtifact.return_value.title = ''

      # Link attributes on ArtifactPayload mock instance so payload.text etc
      # works.
      artifact_pb2.ArtifactPayload.return_value.text = (
          artifact_pb2.TextArtifact.return_value
      )
      artifact_pb2.ArtifactPayload.return_value.url = (
          artifact_pb2.UrlArtifact.return_value
      )
      artifact_pb2.ArtifactPayload.return_value.code_source = (
          artifact_pb2.CodeSourceArtifact.return_value
      )

  def test_process_payload_text(self):
    kwargs = {'text': 'some text'}
    if is_mocked:
      # Configure mock return values so that the hasattr checks evaluate
      # correctly.
      artifact_pb2.TextArtifact.return_value.text = 'some text'
      artifact_pb2.TextArtifact.return_value.title = 'My Text Artifact'

    payload, additional_info, metadata = experiment._process_artifact_payload(
        title='My Text Artifact',
        kwargs=kwargs,
        default_url='http://default',
    )

    self.assertEqual(payload.text.text, 'some text')
    self.assertEqual(payload.text.title, 'My Text Artifact')

    if is_mocked:
      artifact_pb2.TextArtifact.assert_called_once_with(
          text='some text', title='My Text Artifact'
      )
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          text=artifact_pb2.TextArtifact.return_value
      )
      self.assertEqual(payload, artifact_pb2.ArtifactPayload.return_value)

      # Verify AdditionalInfo was constructed as expected
      artifact_pb2.Artifact.AdditionalInfo.assert_called_once_with(
          text='some text', title='My Text Artifact'
      )
      self.assertEqual(
          additional_info, artifact_pb2.Artifact.AdditionalInfo.return_value
      )
    else:
      self.assertEqual(additional_info.text, 'some text')
      self.assertEqual(additional_info.title, 'My Text Artifact')

    self.assertEqual(metadata['url'], 'http://default')
    self.assertNotIn('text', kwargs)

  def test_process_payload_url(self):
    kwargs = {'url': 'http://foo'}
    if is_mocked:
      artifact_pb2.UrlArtifact.return_value.url = 'http://foo'

    payload, additional_info, metadata = experiment._process_artifact_payload(
        title='My Url Artifact',
        kwargs=kwargs,
        default_url='http://default',
    )

    self.assertEqual(payload.url.url, 'http://foo')

    if is_mocked:
      artifact_pb2.UrlArtifact.assert_called_once_with(url='http://foo')
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          url=artifact_pb2.UrlArtifact.return_value
      )
      self.assertEqual(payload, artifact_pb2.ArtifactPayload.return_value)

    self.assertIsNone(additional_info)
    self.assertEqual(metadata['url'], 'http://foo')
    self.assertNotIn('url', kwargs)

  def test_process_payload_code_source(self):
    kwargs = {'code_source': 'print("hello")', 'url': 'http://code_url'}
    if is_mocked:
      artifact_pb2.CodeSourceArtifact.return_value.code_block = 'print("hello")'
      artifact_pb2.CodeSourceArtifact.return_value.code_url = 'http://code_url'
      artifact_pb2.CodeSourceArtifact.return_value.title = 'My Code Artifact'

    payload, additional_info, metadata = experiment._process_artifact_payload(
        title='My Code Artifact',
        kwargs=kwargs,
        default_url='http://default',
    )

    self.assertEqual(payload.code_source.code_block, 'print("hello")')
    self.assertEqual(payload.code_source.code_url, 'http://code_url')

    if is_mocked:
      artifact_pb2.CodeSourceArtifact.assert_called_once_with(
          code_block='print("hello")',
          code_url='http://code_url',
          title='My Code Artifact',
      )
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          code_source=artifact_pb2.CodeSourceArtifact.return_value
      )
      self.assertEqual(payload, artifact_pb2.ArtifactPayload.return_value)

      # Verify AdditionalInfo was constructed as expected
      artifact_pb2.Artifact.AdditionalInfo.assert_called_once_with(
          code_block='print("hello")', title='My Code Artifact'
      )
      self.assertEqual(
          additional_info, artifact_pb2.Artifact.AdditionalInfo.return_value
      )
    else:
      self.assertEqual(additional_info.code_block, 'print("hello")')
      self.assertEqual(additional_info.title, 'My Code Artifact')

    self.assertEqual(metadata['url'], 'http://code_url')
    self.assertNotIn('code_source', kwargs)
    self.assertNotIn('url', kwargs)

  def test_process_payload_multiple_errors(self):
    kwargs = {'text': 'some text', 'url': 'http://foo'}
    with self.assertRaisesRegex(ValueError, 'Only one artifact payload type'):
      experiment._process_artifact_payload(
          title='Error',
          kwargs=kwargs,
          default_url='http://default',
      )

  def test_process_payload_and_additional_info_error(self):
    if is_mocked:
      legacy_proto = mock.MagicMock(name='AdditionalInfoInstance')
      legacy_proto.WhichOneof.return_value = 'text'
      legacy_proto.text = 'legacy text'
      legacy_proto.title = 'Legacy Proto'
    else:
      legacy_proto = artifact_pb2.Artifact.AdditionalInfo(
          title='Legacy Proto',
          text='legacy text',
      )

    kwargs = {'text': 'new text'}
    with self.assertRaisesRegex(ValueError, 'Cannot specify both legacy'):
      experiment._process_artifact_payload(
          title='Error',
          kwargs=kwargs,
          default_url='http://default',
          additional_info_input=legacy_proto,
      )

  def test_legacy_additional_info_dict_text(self):
    kwargs = {}
    if is_mocked:
      artifact_pb2.TextArtifact.return_value.text = 'legacy text'
      artifact_pb2.TextArtifact.return_value.title = 'Legacy Text'

    payload, additional_info, _ = experiment._process_artifact_payload(
        title='Legacy Text',
        kwargs=kwargs,
        default_url='http://default',
        additional_info_input={'title': 'Legacy Dict', 'text': 'legacy text'},
    )

    self.assertEqual(payload.text.text, 'legacy text')
    self.assertEqual(payload.text.title, 'Legacy Text')

    if is_mocked:
      artifact_pb2.TextArtifact.assert_called_once_with(
          text='legacy text', title='Legacy Text'
      )
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          text=artifact_pb2.TextArtifact.return_value
      )
      self.assertEqual(payload, artifact_pb2.ArtifactPayload.return_value)

      # Verify AdditionalInfo was constructed as expected
      artifact_pb2.Artifact.AdditionalInfo.assert_called_once_with(
          text='legacy text', title='Legacy Text'
      )
      self.assertEqual(
          additional_info, artifact_pb2.Artifact.AdditionalInfo.return_value
      )
    else:
      self.assertEqual(additional_info.text, 'legacy text')
      self.assertEqual(additional_info.title, 'Legacy Text')

  def test_legacy_additional_info_proto_code(self):
    if is_mocked:
      legacy_proto = mock.MagicMock(name='AdditionalInfoInstance')
      legacy_proto.WhichOneof.return_value = 'code_block'
      legacy_proto.code_block = 'my code'
      legacy_proto.title = 'Legacy Proto'

      artifact_pb2.CodeSourceArtifact.return_value.code_block = 'my code'
      artifact_pb2.CodeSourceArtifact.return_value.code_url = (
          'http://sibling_url'
      )
      artifact_pb2.CodeSourceArtifact.return_value.title = 'Legacy Title'
    else:
      legacy_proto = artifact_pb2.Artifact.AdditionalInfo(
          title='Legacy Proto',
          code_block='my code',
      )

    kwargs = {'url': 'http://sibling_url'}

    payload, additional_info, _ = experiment._process_artifact_payload(
        title='Legacy Title',
        kwargs=kwargs,
        default_url='http://default',
        additional_info_input=legacy_proto,
    )

    self.assertEqual(payload.code_source.code_block, 'my code')
    self.assertEqual(payload.code_source.code_url, 'http://sibling_url')

    if is_mocked:
      artifact_pb2.CodeSourceArtifact.assert_called_once_with(
          code_block='my code',
          code_url='http://sibling_url',
          title='Legacy Title',
      )
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          code_source=artifact_pb2.CodeSourceArtifact.return_value
      )
      self.assertEqual(payload, artifact_pb2.ArtifactPayload.return_value)

      # Verify AdditionalInfo was constructed as expected
      artifact_pb2.Artifact.AdditionalInfo.assert_called_once_with(
          code_block='my code', title='Legacy Title'
      )
      self.assertEqual(
          additional_info, artifact_pb2.Artifact.AdditionalInfo.return_value
      )
    else:
      self.assertEqual(additional_info.code_block, 'my code')
      self.assertEqual(additional_info.title, 'Legacy Title')


class CreateArtifactTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    if is_mocked:
      artifact_pb2.Artifact.reset_mock()
      artifact_pb2.Artifact.AdditionalInfo.reset_mock()
      artifact_pb2.ArtifactPayload.reset_mock()
      artifact_pb2.TextArtifact.reset_mock()
      artifact_pb2.UrlArtifact.reset_mock()
      artifact_pb2.CodeSourceArtifact.reset_mock()
      experiment_state_service_pb2.CreateArtifactRequest.reset_mock()

      artifact_pb2.TextArtifact.return_value.text = ''
      artifact_pb2.TextArtifact.return_value.title = ''

  def test_create_artifact_unexpected_args(self):
    mock_stub = mock.MagicMock()
    with self.assertRaisesRegex(TypeError, 'unexpected keyword arguments'):
      experiment._create_artifact(
          stub=mock_stub,
          parent_resource_name='experiments/123',
          text='some text',
          invalid_arg='value',
      )

  def test_create_artifact_success(self):
    mock_stub = mock.MagicMock()

    if is_mocked:
      # We mock the entire Artifact proto returned by the stub.
      mock_proto_response = mock.MagicMock(name='ArtifactProtoResponse')
      mock_proto_response.name = 'experiments/123/artifacts/456'

      # mock WhichOneof to return something other than None so it has a type
      mock_proto_response.payload.WhichOneof.return_value = 'text'
      mock_text_inst = mock.MagicMock(name='TextArtifactInstance')
      mock_text_inst.text = 'my text return value'
      mock_text_inst.title = 'My Title'
      mock_proto_response.payload.text = mock_text_inst

      artifact_pb2.TextArtifact.return_value.text = 'hello'
      artifact_pb2.TextArtifact.return_value.title = 'My Title'
    else:
      mock_proto_response = artifact_pb2.Artifact(
          name='experiments/123/artifacts/456',
          payload=artifact_pb2.ArtifactPayload(
              text=artifact_pb2.TextArtifact(
                  text='my text return value', title='My Title'
              )
          ),
      )

    mock_stub.create_artifact.return_value = mock_proto_response

    art = experiment._create_artifact(
        stub=mock_stub,
        parent_resource_name='experiments/123',
        text='hello',
        title='My Title',
    )

    self.assertEqual(art.id, 456)
    self.assertEqual(art.name, 'experiments/123/artifacts/456')
    self.assertEqual(art.type, 'text')
    self.assertEqual(art.text.text, 'my text return value')

    if is_mocked:
      # Check constructors
      artifact_pb2.TextArtifact.assert_called_once_with(
          text='hello', title='My Title'
      )
      artifact_pb2.ArtifactPayload.assert_called_once_with(
          text=artifact_pb2.TextArtifact.return_value
      )
      artifact_pb2.Artifact.assert_called_once()
      experiment_state_service_pb2.CreateArtifactRequest.assert_called_once_with(
          parent='experiments/123', artifact=artifact_pb2.Artifact.return_value
      )

      # Check stub invocation
      mock_stub.create_artifact.assert_called_once_with(
          experiment_state_service_pb2.CreateArtifactRequest.return_value
      )
    else:
      # Check stub invocation in real mode
      mock_stub.create_artifact.assert_called_once()
      called_request = mock_stub.create_artifact.call_args[0][0]
      self.assertEqual(called_request.parent, 'experiments/123')
      self.assertEqual(called_request.artifact.payload.text.text, 'hello')
      self.assertEqual(called_request.artifact.payload.text.title, 'My Title')


if __name__ == '__main__':
  absltest.main()
