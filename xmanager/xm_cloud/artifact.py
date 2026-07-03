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

"""Artifact management logic for XManager on Cloud."""

import os
from typing import Any, Optional, Sequence

from google.protobuf import message

from google.protobuf import field_mask_pb2
from xmanager_cloud.experiment_state_server import experiment_state_api
from xmanager_cloud.experiment_state_server.proto import api_pb2 as experiment_state_service_pb2
from xmanager_cloud.experiment_state_server.proto import artifact_pb2
from xmanager_cloud.experiment_state_server.proto import url_pb2


def _get_xmanager_ui_url() -> str:
  return os.environ.get('XMANAGER_UI_URL', '')


class Artifact:
  """Wrapper for Artifact proto."""

  def __init__(self, proto: artifact_pb2.Artifact):
    self._proto = proto

  @property
  def name(self) -> str:
    return self._proto.name

  @property
  def id(self) -> int:
    # Assuming name is in format experiments/{XID}/artifacts/{AID}
    if not self._proto.name:
      # This should never happen.
      raise ValueError('Artifact name is empty.')
    return int(self._proto.name.split('/')[-1])

  @property
  def type(self) -> str:
    """Returns the name of the active field in the payload oneof."""
    return self._proto.payload.WhichOneof('type') or 'unspecified'

  @property
  def lifecycle_phase(self) -> str:
    return artifact_pb2.Artifact.LifecyclePhase.Name(
        self._proto.lifecycle_phase
    )

  def __repr__(self):
    return (
        f"Artifact(id={self.id}, name='{self.name}', type={self.type}, "
        f'lifecycle_phase={self.lifecycle_phase})'
    )

  @property
  def payload_value(self) -> Optional[Any]:
    """Returns the value of the active payload field."""
    if self.type != 'unspecified':
      return getattr(self._proto.payload, self.type)
    return None

  @property
  def additional_info(self) -> Optional[Any]:
    """Returns the deprecated additional_info field, if present."""
    if self._proto.HasField('additional_info'):
      return self._proto.additional_info
    return None

  @property
  def _payload_keys(self) -> set[str]:
    payload_descriptor = self._proto.payload.DESCRIPTOR
    oneof_descriptor = payload_descriptor.oneofs_by_name['type']
    return {f.name for f in oneof_descriptor.fields}

  def __getattr__(self, name: str) -> Any:
    payload_keys = self._payload_keys

    # 1. Exact match with active type (e.g., artifact.text)
    if self.type == name:
      return self.payload_value

    # 2. Match with _artifact suffix (e.g., artifact.text_artifact)
    if name.endswith('_artifact'):
      payload_type = name[:-9]
      if self.type == payload_type:
        return self.payload_value
      # If it's a valid payload type but not the active one, return None.
      if payload_type in payload_keys:
        return None

    # 3. If it's a valid payload key but not the active type, return None.
    if name in payload_keys:
      return None

    raise AttributeError(
        f"'{self.__class__.__name__}' object has no attribute '{name}'"
    )


def _convert_legacy_additional_info(
    title: str,
    additional_info_input: Any,
    kwargs: dict[str, Any],
    legacy_url: Optional[str],
) -> str:
  """Converts legacy additional_info to new payload kwargs in place.

  Args:
    title: The default title to fall back to.
    additional_info_input: The legacy additional_info dict or proto.
    kwargs: The keyword arguments to update in place.
    legacy_url: Sibling url argument if any.

  Returns:
    The title (which may be overridden by additional_info_input).
  """
  if isinstance(additional_info_input, dict):
    additional_info_title = additional_info_input.get('title', '')
    additional_info_text = additional_info_input.get('text', '')
    additional_info_code_block = additional_info_input.get('code_block', '')
  else:
    additional_info_title = additional_info_input.title
    content_type = additional_info_input.WhichOneof('content')
    additional_info_text = (
        additional_info_input.text if content_type == 'text' else ''
    )
    additional_info_code_block = (
        additional_info_input.code_block if content_type == 'code_block' else ''
    )

  title = title or additional_info_title
  if additional_info_text:
    kwargs['text'] = additional_info_text
  elif additional_info_code_block:
    kwargs['code_source'] = additional_info_code_block
    if legacy_url:
      kwargs['url'] = legacy_url
  else:
    raise ValueError('legacy additional_info must contain text or code_block')
  return title


def _build_payload_proto(
    payload_key: str,
    field_desc: Any,
    title: str,
    kwargs: dict[str, Any],
) -> message.Message:
  """Builds the specific payload proto class based on the payload key."""
  primitive_field_map = {
      'TextArtifact': 'text',
      'UrlArtifact': 'url',
      'CodeSourceArtifact': 'code_block',
  }

  proto_class = getattr(artifact_pb2, field_desc.message_type.name)
  val = kwargs.pop(payload_key)

  args = {}
  if isinstance(val, dict):
    args.update(val)
  else:
    class_name = proto_class.__name__
    if class_name in primitive_field_map:
      field_name = primitive_field_map[class_name]
      if field_name == 'url' and hasattr(val, 'url'):
        args[field_name] = val.url
      else:
        args[field_name] = val
    else:
      raise ValueError(
          f'{payload_key} payload must be a dictionary, got {type(val)}'
      )

  # Special case: sibling 'url' maps to 'code_url' for code_source
  if payload_key == 'code_source' and 'url' in kwargs:
    passed_url = kwargs.pop('url')
    if hasattr(passed_url, 'url'):
      passed_url_str = passed_url.url
    else:
      passed_url_str = passed_url
    if 'code_url' not in args or not args['code_url']:
      args['code_url'] = passed_url_str

  # Pull other fields that belong to this proto from kwargs
  for field in field_desc.message_type.fields:
    if field.name in kwargs:
      args[field.name] = kwargs.pop(field.name)
    # Automatically fill title if the proto expects it and it's not already set
    if field.name == 'title' and 'title' not in args and title:
      args['title'] = title

  # Validate common required fields
  if payload_key == 'text' and not args.get('text'):
    raise ValueError('text is required for text artifact')
  if payload_key == 'code_source' and not args.get('code_block'):
    raise ValueError('code_block is required for code source artifact')
  if payload_key == 'url' and not args.get('url'):
    raise ValueError('url is required for url artifact')

  return proto_class(**args)


def _extract_legacy_additional_info(
    payload_proto: message.Message, title: str
) -> Optional[artifact_pb2.Artifact.AdditionalInfo]:
  """Extracts legacy AdditionalInfo from the payload proto for backward compatibility."""
  additional_info_args = {}
  if hasattr(payload_proto, 'title') and payload_proto.title:
    additional_info_args['title'] = payload_proto.title
  elif title:
    additional_info_args['title'] = title

  if hasattr(payload_proto, 'text') and payload_proto.text:
    additional_info_args['text'] = payload_proto.text
    return artifact_pb2.Artifact.AdditionalInfo(**additional_info_args)
  elif hasattr(payload_proto, 'code_block') and payload_proto.code_block:
    additional_info_args['code_block'] = payload_proto.code_block
    return artifact_pb2.Artifact.AdditionalInfo(**additional_info_args)
  return None


def _extract_metadata(
    payload_key: str,
    payload_proto: message.Message,
    field_desc: Any,
    default_url: str,
) -> dict[str, Any]:
  """Extracts metadata dictionary from the payload proto."""
  metadata = {
      'url': default_url,
      'display_name': '',
      'icon': '',
  }
  # Reflective Metadata URL Extraction
  url_fields = [
      f.name
      for f in field_desc.message_type.fields
      if f.name == 'url' or f.name.endswith('_url')
  ]
  for url_field in url_fields:
    url_val = getattr(payload_proto, url_field, None)
    if url_val:
      metadata['url'] = url_val
      break

  if payload_key == 'url':
    if hasattr(payload_proto, 'display_name') and payload_proto.display_name:
      metadata['display_name'] = payload_proto.display_name
    if hasattr(payload_proto, 'icon') and payload_proto.icon:
      metadata['icon'] = payload_proto.icon

  return metadata


def _process_artifact_payload(
    title: str,
    kwargs: dict[str, Any],
    default_url: str,
    additional_info_input: Optional[Any] = None,
) -> tuple[
    artifact_pb2.ArtifactPayload,
    Optional[artifact_pb2.Artifact.AdditionalInfo],
    dict[str, Any],
]:
  """Helper to process and construct the artifact payload and metadata."""
  legacy_url = None
  has_code_block = False
  if additional_info_input:
    if isinstance(additional_info_input, dict):
      has_code_block = 'code_block' in additional_info_input
    else:
      has_code_block = (
          additional_info_input.WhichOneof('content') == 'code_block'
      )

  if has_code_block and 'url' in kwargs:
    # Extract url to avoid conflicts with new strong-typed payload checks
    legacy_url = kwargs.pop('url')

  payload_descriptor = artifact_pb2.ArtifactPayload.DESCRIPTOR
  oneof_descriptor = payload_descriptor.oneofs_by_name['type']
  payload_fields = {f.name: f for f in oneof_descriptor.fields}
  payload_keys = set(payload_fields.keys())

  present_keys = payload_keys.intersection(kwargs.keys())

  # Special case: 'url' is allowed alongside 'code_source' (maps to code_url)
  if 'code_source' in present_keys and 'url' in kwargs:
    present_keys_for_count = present_keys - {'url'}
  else:
    present_keys_for_count = present_keys

  if len(present_keys_for_count) > 1:
    raise ValueError(
        'Only one artifact payload type can be specified. Found:'
        f' {present_keys}'
    )
  if additional_info_input and present_keys_for_count:
    raise ValueError(
        "Cannot specify both legacy 'additional_info' and new payload "
        f'arguments ({present_keys_for_count}).'
    )

  # Unify Legacy Path: Convert additional_info to kwargs internally
  if additional_info_input and not present_keys_for_count:
    title = _convert_legacy_additional_info(
        title, additional_info_input, kwargs, legacy_url
    )

    # Re-evaluate present keys after conversion
    present_keys = payload_keys.intersection(kwargs.keys())
    if 'code_source' in present_keys and 'url' in kwargs:
      present_keys_for_count = present_keys - {'url'}
    else:
      present_keys_for_count = present_keys

  if not present_keys_for_count:
    raise ValueError(
        'Could not determine artifact payload type. '
        f'Must provide one of: {payload_keys} '
        "or a legacy 'additional_info' containing text/code_block."
    )

  payload_key = list(present_keys_for_count)[0]
  field_desc = payload_fields[payload_key]

  payload_proto = _build_payload_proto(payload_key, field_desc, title, kwargs)
  payload = artifact_pb2.ArtifactPayload(**{payload_key: payload_proto})

  additional_info = _extract_legacy_additional_info(payload_proto, title)
  metadata = _extract_metadata(
      payload_key, payload_proto, field_desc, legacy_url or default_url
  )

  return payload, additional_info, metadata


def _map_lifecycle_phase(
    lifecycle_phase: Optional[Any],
) -> artifact_pb2.Artifact.LifecyclePhase:
  """Maps the lifecycle phase to the corresponding  proto enum value.

  This maps lifecycle_phase (which can be a string enum name like "INPUT" or
  "LIFECYCLE_PHASE_INPUT", or an integer value) to the corresponding proto
  enum value. If it's None, it defaults to INPUT.

  Args:
    lifecycle_phase: The lifecycle phase to map.

  Returns:
    The corresponding proto enum value.
  """
  if isinstance(lifecycle_phase, str):
    try:
      return artifact_pb2.Artifact.LifecyclePhase.Value(lifecycle_phase)
    except ValueError:
      if lifecycle_phase.startswith('LIFECYCLE_PHASE_'):
        short_name = lifecycle_phase[len('LIFECYCLE_PHASE_') :]
        try:
          return artifact_pb2.Artifact.LifecyclePhase.Value(short_name)
        except ValueError:
          raise ValueError(
              f'Invalid lifecycle_phase: {lifecycle_phase}'
          ) from None
      else:
        raise ValueError(
            f'Invalid lifecycle_phase: {lifecycle_phase}'
        ) from None
  elif isinstance(lifecycle_phase, int):
    return lifecycle_phase
  elif lifecycle_phase is None:
    # Default to INPUT if not specified
    return artifact_pb2.Artifact.LifecyclePhase.INPUT
  else:
    raise TypeError(f'Invalid lifecycle_phase type: {type(lifecycle_phase)}')


def create_artifact(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    parent_resource_name: str,
    **kwargs,
) -> Artifact:
  """Creates an artifact for the given parent resource (experiment/work unit)."""
  # 1. Extract common fields
  lifecycle_phase = kwargs.pop('lifecycle_phase', 'INPUT')
  mime_type = kwargs.pop('mime_type', None)
  acls = kwargs.pop('acls', None)
  tags = kwargs.pop('tags', None)
  notes = kwargs.pop('notes', None)
  data_type = kwargs.pop('data_type', None)

  title = kwargs.pop('title', '')
  additional_info_input = kwargs.pop('additional_info', None)

  # Construct default URL pointing to the experiment/work unit in the UI
  ui_url = _get_xmanager_ui_url()
  default_url = f'{ui_url}/{parent_resource_name}'

  # 2. Process payload and additional_info using helper
  payload, additional_info, metadata = _process_artifact_payload(
      title, kwargs, default_url, additional_info_input
  )

  # Check for unknown arguments remaining
  if kwargs:
    raise TypeError(
        f'create_artifact() got unexpected keyword arguments: {kwargs.keys()}'
    )

  # 3. Map lifecycle phase to proto.
  lifecycle_phase_val = _map_lifecycle_phase(lifecycle_phase)

  # 4. Construct Artifact Proto
  url_proto = url_pb2.Url(url=metadata['url'])
  if metadata['display_name']:
    url_proto.display_name = metadata['display_name']
  if metadata['icon']:
    url_proto.icon = metadata['icon']

  artifact_proto = artifact_pb2.Artifact(
      # Populate deprecated fields for backward compatibility
      url=url_proto,
      lifecycle_phase=lifecycle_phase_val,
      mime_type=mime_type or '',
      acls=acls,
      tags=tags,
      notes=notes,
      payload=payload,  # Use the new payload
  )
  if data_type is not None:
    artifact_proto.data_type = data_type
  if additional_info:
    artifact_proto.additional_info.CopyFrom(
        additional_info
    )  # Populate legacy field

  request = experiment_state_service_pb2.CreateArtifactRequest(
      parent=parent_resource_name,
      artifact=artifact_proto,
  )

  response_proto = stub.create_artifact(request)
  return Artifact(response_proto)


def list_artifacts(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    parent_resource_name: str,
    *,
    filter_query: str | None = None,
    strongly_consistent: bool = False,
    page_size: int = 100,
    page_token: str | None = None,
) -> tuple[Sequence[artifact_pb2.Artifact], str | None]:
  """Lists artifacts for the given parent resource (experiment or work unit).

  Args:
    stub: The experiment state stub.
    parent_resource_name: The resource name of the parent (either an Experiment
      or a WorkUnit). Format is one of the following: "experiments/{XID}"
      "experiments/{XID}/workUnits/{WID}"
    filter_query: The filter query to use for the list operation.
    strongly_consistent: Whether to use strongly consistent reads for the list
      operation. True -> Strongly consistent reads, but higher latency and more
      expensive call. False -> Eventually consistent reads (<15s staleness),
      lower latency and database load.
    page_size: The page size to use for the list operation.
    page_token: The page token to use for the list operation.

  Returns:
    A tuple (artifacts, next_page_token), where artifacts is a list of
    artifacts and next_page_token is the token for the next page.
  """
  request = experiment_state_service_pb2.ListArtifactsRequest(
      parent=parent_resource_name,
      filter=filter_query,
      page_token=page_token,
      page_size=page_size,
  )
  if strongly_consistent:
    response = stub.list_artifacts(request)
  else:
    response = stub.search_artifacts(request)

  return list(response.artifacts), response.next_page_token


def delete_artifact(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    artifact_name: str,
) -> None:
  """Deletes an artifact.

  Args:
    stub: The experiment state stub.
    artifact_name: The resource name of the artifact. Format is
      "artifacts/{aid}"
  """
  stub.delete_artifact(
      experiment_state_service_pb2.DeleteArtifactRequest(name=artifact_name)
  )


def update_artifact(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    artifact_resource_name: str,
    **kwargs,
) -> Artifact:
  """Updates an artifact in the Experiment State Service database."""
  parent_resource_name = artifact_resource_name.split('/artifacts')[0]

  # 1. Extract common fields
  lifecycle_phase = kwargs.pop('lifecycle_phase', None)
  mime_type = kwargs.pop('mime_type', None)
  acls = kwargs.pop('acls', None)
  tags = kwargs.pop('tags', None)
  notes = kwargs.pop('notes', None)
  data_type = kwargs.pop('data_type', None)

  url_legacy = kwargs.pop('url', None)
  if url_legacy is not None and not isinstance(url_legacy, url_pb2.Url):
    kwargs['url'] = url_legacy
    url_legacy = None

  title = kwargs.pop('title', '')
  additional_info_input = kwargs.pop('additional_info', None)

  # Check if any payload keys remain in kwargs,
  # or if additional_info was provided
  payload = None
  additional_info = None
  metadata = None

  payload_descriptor = artifact_pb2.ArtifactPayload.DESCRIPTOR
  oneof_descriptor = payload_descriptor.oneofs_by_name['type']
  payload_keys = {f.name for f in oneof_descriptor.fields}
  has_payload_args = bool(payload_keys.intersection(kwargs.keys()))

  if has_payload_args or additional_info_input:
    ui_url = _get_xmanager_ui_url()
    default_url = f'{ui_url}/{parent_resource_name}'
    payload, additional_info, metadata = _process_artifact_payload(
        title, kwargs, default_url, additional_info_input
    )

  if kwargs:
    raise TypeError(
        f'update_artifact() got unexpected keyword arguments: {kwargs.keys()}'
    )

  # Construct update dictionary and paths
  update_kwargs = {}
  paths = []

  if lifecycle_phase is not None:
    update_kwargs['lifecycle_phase'] = _map_lifecycle_phase(lifecycle_phase)
    paths.append('lifecycle_phase')
  if mime_type is not None:
    update_kwargs['mime_type'] = mime_type
    paths.append('mime_type')
  if acls is not None:
    update_kwargs['acls'] = acls
    paths.append('acls')
  if tags is not None:
    update_kwargs['tags'] = tags
    paths.append('tags')
  if notes is not None:
    update_kwargs['notes'] = notes
    paths.append('notes')
  if data_type is not None:
    update_kwargs['data_type'] = data_type
    paths.append('data_type')

  if payload is not None:
    update_kwargs['payload'] = payload
    paths.append('payload')

    # Also update the deprecated top-level url field if we updated the payload
    url_proto = url_pb2.Url(url=metadata['url'])
    if metadata['display_name']:
      url_proto.display_name = metadata['display_name']
    if metadata['icon']:
      url_proto.icon = metadata['icon']
    update_kwargs['url'] = url_proto
    paths.append('url')

  if additional_info is not None:
    update_kwargs['additional_info'] = additional_info
    paths.append('additional_info')

  if url_legacy is not None:
    update_kwargs['url'] = url_legacy
    paths.append('url')

  if not paths:
    raise ValueError('At least one field must be specified for update.')

  response_proto = stub.update_artifact(
      experiment_state_service_pb2.UpdateArtifactRequest(
          artifact=artifact_pb2.Artifact(
              name=artifact_resource_name,
              **update_kwargs,
          ),
          update_mask=field_mask_pb2.FieldMask(paths=paths),
      )
  )
  return Artifact(response_proto)
