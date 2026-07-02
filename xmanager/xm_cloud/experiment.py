"""Implementation of an experiment running on the XManager on Cloud backend."""

from collections.abc import Iterable, Mapping, Sequence
from concurrent import futures
import datetime
import functools
import itertools
import logging
import os
from typing import Any, Awaitable, Callable, Optional, cast

from absl import flags
import attr
from google.protobuf import message
import termcolor
from xmanager import xm
from xmanager.xm import async_packager
from xmanager.xm import id_predictor as xm_id_predictor
from xmanager.xm_cloud import executor as xm_cloud_executor
from xmanager.xm_cloud import snapshot_code
from xmanager.xm_cloud.packaging import router as packaging_router
from xmanager.xm_local import executables

from google.longrunning import operations_pb2
from google.protobuf import field_mask_pb2
from xmanager_cloud.experiment_state_server import experiment_state_api
from xmanager_cloud.experiment_state_server.proto import api_pb2 as experiment_state_service_pb2
from xmanager_cloud.experiment_state_server.proto import artifact_pb2
from xmanager_cloud.experiment_state_server.proto import experiment_pb2
from xmanager_cloud.experiment_state_server.proto import priority_pb2
from xmanager_cloud.experiment_state_server.proto import resources_pb2
from xmanager_cloud.experiment_state_server.proto import status_message_pb2
from xmanager_cloud.experiment_state_server.proto import status_pb2
from xmanager_cloud.experiment_state_server.proto import url_pb2
from xmanager_cloud.experiment_state_server.proto import work_unit_pb2
from xmanager_cloud.xid_service.proto import messages_pb2 as xid_messages_pb2

FLAGS = flags.FLAGS
_WELCOME_MESSAGE = """
Welcome to XManager on Cloud!"""

_DEFAULT_QUEUE_NAME = 'cpu-queue'

_TPU_TYPE_TO_GKE_ACCELERATOR = {
    xm.ResourceType.TPU_V4: 'tpu-v4-podslice',
    xm.ResourceType.TPU_V5E: 'tpu-v5-lite-podslice',
    xm.ResourceType.TPU_V5P: 'tpu-v5p-slice',
    xm.ResourceType.TPU_V6E: 'tpu-v6e-slice',
    xm.ResourceType.TPU_V7X: 'tpu7x-standard-4t',
}


def _tpu_type_to_gke_accelerator(
    resource_type: xm.ResourceType,
    topology: xm.Topology,
) -> str:
  """Converts an XM TPU resource type and topology to a GKE accelerator type.

  Args:
    resource_type: The XManager TPU resource type.
    topology: The XManager TPU topology.

  Returns:
    The GKE accelerator type.

  Raises:
    ValueError: If the TPU type is not supported for GKE.
  """
  accelerator = _TPU_TYPE_TO_GKE_ACCELERATOR.get(resource_type)
  if accelerator is not None:
    return accelerator
    if resource_type == xm.ResourceType.TPU_V3:
    if topology.chip_count <= 8:
      return 'tpu-v3-device'
    return 'tpu-v3-slice'
  raise ValueError(f'Unsupported TPU type for GKE: {resource_type!r}')


def _get_xmanager_ui_url() -> str:
  return os.environ.get('XMANAGER_UI_URL', '')


def _get_kueue_queue_name() -> str:
  if env_var := os.environ.get('XMANAGER_CPU_QUEUE_NAME'):
    return env_var
  return _DEFAULT_QUEUE_NAME


@attr.s(auto_attribs=True)
class WorkUnitSettings(xm.WorkUnitRole):
  """Various settings and attributes for a work unit.

  Attributes:
    work_unit_type: The type of work unit (standard or auxiliary).
    scheduling_constraints: The scheduling constraints of the work unit (e.g.
      Kueue queue name).
    priority: The priority of the work unit, if different from the experiment
      priority.
    tags: The tags of the work unit.
    notes: The notes of the work unit.
    urls: The URLs that should be displayed for the work unit in the UI.
  """

  scheduling_constraints: work_unit_pb2.SchedulingConstraints = (
      work_unit_pb2.SchedulingConstraints(queue_name=_get_kueue_queue_name())
  )
  work_unit_type: work_unit_pb2.WorkUnit.WorkUnitType = (
      work_unit_pb2.WorkUnit.WorkUnitType.STANDARD
  )
  priority: priority_pb2.Priority | None = None
  tags: Sequence[str] | None = None
  notes: str | None = None
  urls: Sequence[url_pb2.Url] | None = None


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


class XManagerCloudExperiment(xm.Experiment):
  """An experiment running on the XManager on Cloud backend."""

  _async_packager = async_packager.AsyncPackager(packaging_router.package)

  def __init__(
      self,
      experiment_state_stub: experiment_state_api.ExperimentStateGRpcApi,
      experiment_proto: experiment_pb2.Experiment,
      id_predictor: xm_id_predictor.Predictor | None = None,
  ):
    super().__init__()
    self._experiment_state_stub = experiment_state_stub
    self._experiment_proto = experiment_proto
    self._id_predictor = id_predictor

  @property
  def id(self) -> int:
    """The unique ID of the experiment (a.k.a. XID)."""
    return self._parse_experiment_id(self._experiment_proto.name)

  def _parse_experiment_id(self, experiment_name: str) -> int:
    return int(experiment_name.split('/')[-1])

  @property
  def resource_name(self) -> str:
    """The resource name of the experiment (i.e. experiments/{XID})."""
    return self._experiment_proto.name

  @property
  def title(self) -> str:
    """The title of the experiment."""
    return self._experiment_proto.title

  @property
  def author(self) -> str:
    """The username/IAM principal identifier of the person who created the experiment."""
    return self._experiment_proto.creator

  @property
  def creation_time(self) -> datetime.datetime:
    """The time when the experiment was created."""
    return self._experiment_proto.create_time.ToDatetime()

  @property
  def priority(self) -> priority_pb2.Priority:
    """The priority of the experiment."""
    return self._experiment_proto.priority

  @property
  def status(self) -> status_pb2.ExperimentStatus:
    """The current status of the experiment."""
    return self._experiment_proto.status

  @property
  def work_units(self) -> Mapping[int, xm.ExperimentUnit]:
    """The first page of work units created via self.add().

    To fetch all work units, use `list_all_work_units()`.
    """

    work_units, _ = self._list_work_units(
        filter_query=None,
        strongly_consistent=False,
        page_token=None,
    )
    return {work_unit.work_unit_id: work_unit for work_unit in work_units}

  def list_all_work_units(self) -> Mapping[int, xm.ExperimentUnit]:
    """Fetches all work units for the experiment by paginating through results.

    Warning: This can be slow for experiments with a very large number of
    work units.

    Returns:
      A mapping from work unit ID to WorkUnit.
    """
    all_work_units = []
    next_page_token = None
    while True:
      work_units, next_page_token = self._list_work_units(
          filter_query=None,
          strongly_consistent=False,
          page_token=next_page_token,
      )
      all_work_units.extend(work_units)
      if next_page_token is None:
        break
    return {work_unit.work_unit_id: work_unit for work_unit in all_work_units}

  def get_work_unit_count(self) -> int:
    """Number of work units on the first page of the experiment."""
    return len(self.list_all_work_units())

  def add(
      self,
      job,
      args=None,
      *,
      settings: WorkUnitSettings | None = None,
      identity: str = '',
  ):
    """Adds a job to the experiment."""
    return super().add(
        job,
        args=args,
        role=settings,
        identity=identity,
    )

  def _list_work_units(
      self,
      *,
      filter_query: str | None = None,
      strongly_consistent: bool = False,
      page_size: int = 100,
      page_token: str | None = None,
  ) -> tuple[Sequence[xm.WorkUnit], str | None]:
    """Lists work units for the experiment.

    Args:
      filter_query: The filter query to use for the list operation.
      strongly_consistent: Whether to use strongly consistent reads for the list
        operation. True -> Strongly consistent reads, but higher latency and
        more expensive call. False -> Eventually consistent reads (<15s
        staleness), lower latency and database load.
      page_size: The page size to use for the list operation.
      page_token: The page token to use for the list operation.

    Returns:
      A tuple (work_units, next_page_token), where work_units is a list of
      work units for the experiment and next_page_token is the token for the
      next page.
    """
    request = experiment_state_service_pb2.ListWorkUnitsRequest(
        parent=self.resource_name,
        filter=filter_query,
        page_token=page_token,
        page_size=page_size,
    )
    if strongly_consistent:
      response = self._experiment_state_stub.list_work_units(request)
    else:
      response = self._experiment_state_stub.search_work_units(request)

    work_units = [
        _convert_ess_proto_to_xm_cloud_work_unit(
            self._experiment_state_stub,
            self._experiment_proto,
            work_unit_proto,
        )
        for work_unit_proto in response.work_units
    ]
    return work_units, response.next_page_token

  def _update_experiment(
      self,
      *,
      title: str | None = None,
      priority: priority_pb2.Priority | None = None,
      urls: Sequence[url_pb2.Url] | None = None,
      tags: Sequence[str] | None = None,
      notes: str | None = None,
      acls: xid_messages_pb2.XidAcls | None = None,
  ) -> None:
    """Updates the experiment in the Experiment State Server database."""
    stub = experiment_state_api.get_experiment_state_api()
    kwargs = {
        'title': title,
        'priority': priority,
        'urls': urls,
        'tags': tags,
        'notes': notes,
        'acls': acls,
    }
    paths = [name for name, value in kwargs.items() if value is not None]
    updated_proto = stub.update_experiment(
        experiment_state_service_pb2.UpdateExperimentRequest(
            experiment=experiment_pb2.Experiment(
                name=f'experiments/{self.id}',
                **kwargs,
            ),
            update_mask=field_mask_pb2.FieldMask(paths=paths),
        )
    )
    self._experiment_proto = updated_proto

  def set_title(self, title: str) -> None:
    """Sets the title of the experiment."""
    self._update_experiment(title=title)

  def set_priority(self, priority: priority_pb2.Priority) -> None:
    """Sets the priority of the experiment."""
    self._update_experiment(priority=priority)

  def set_notes(self, notes: str) -> None:
    """Sets the notes of the experiment."""
    self._update_experiment(notes=notes)

  def set_acls(self, acls: xid_messages_pb2.XidAcls) -> None:
    """Sets the ACLs of the experiment."""
    self._update_experiment(acls=acls)

  # Methods for updating the tags of the experiment.

  def set_tags(self, tags: Sequence[str]) -> None:
    """Sets the tags of the experiment.

    Overwrites any existing tags.

    Args:
      tags: The tags to set for the experiment.
    """
    self._update_experiment(tags=tags)

  def add_tags(self, tags: Sequence[str]) -> None:
    """Adds the tags to the experiment.

    Appends to existing tags.

    Args:
      tags: The tags to add to the list of existing tags for the experiment.
    """
    existing_tags: Iterable[str] = self._experiment_proto.tags
    self._update_experiment(tags=list(itertools.chain(existing_tags, tags)))

  def remove_tags(self, tags_to_remove: Sequence[str]) -> None:
    """Removes specified tags from the experiment, if present."""
    existing_tags = self._experiment_proto.tags
    self._update_experiment(
        tags=[tag for tag in existing_tags if tag not in tags_to_remove],
    )

  # Methods for updating the URLs of the experiment.

  def set_urls(self, urls: Sequence[url_pb2.Url]) -> None:
    """Sets the URLs to be displayed for the experiment.

    Overwrites any existing URLs.

    Args:
      urls: The URLs to set for the experiment.
    """
    self._update_experiment(urls=urls)

  def add_urls(self, urls: Sequence[url_pb2.Url]) -> None:
    """Adds the URLs to the experiment.

    Appends to existing URLs.

    Args:
      urls: The URLs to add to the list of URLs for the experiment.
    """
    existing_urls = self._experiment_proto.urls
    self._update_experiment(urls=list(itertools.chain(existing_urls, urls)))

  def remove_urls(self, urls_to_remove: Sequence[url_pb2.Url]) -> None:
    """Removes URLs from the experiment, if present."""
    existing_urls = self._experiment_proto.urls
    self._update_experiment(
        urls=[url for url in existing_urls if url not in urls_to_remove],
    )

  # Methods related to artifacts.
  def create_artifact(self, **kwargs) -> Artifact:
    """Creates an artifact for the experiment.

    Args:
      **kwargs: Keyword arguments defining the artifact. These can include: -
        Common fields: lifecycle_phase, mime_type, acls, tags, notes, title -
        Payload fields (mutually exclusive): text, code_source, url,
        managed_xprof, etc. - Legacy field: additional_info

    Returns:
      The created Artifact wrapper.
    """
    return _create_artifact(
        stub=self._experiment_state_stub,
        parent_resource_name=self.resource_name,
        **kwargs,
    )

  def list_artifacts(
      self,
      *,
      filter_query: str | None = None,
      strongly_consistent: bool = False,
      page_size: int = 100,
      page_token: str | None = None,
  ) -> tuple[Sequence[artifact_pb2.Artifact], str | None]:
    """Lists artifacts for the experiment.

    Args:
      filter_query: The filter query to use for the list operation.
      strongly_consistent: Whether to use strongly consistent reads for the list
        operation. True -> Strongly consistent reads, but higher latency and
        more expensive call. False -> Eventually consistent reads (<15s
        staleness), lower latency and database load.
      page_size: The page size to use for the list operation.
      page_token: The page token to use for the list operation.

    Returns:
      A tuple (artifacts, next_page_token), where artifacts is a list of
      artifacts for the experiment and next_page_token is the token for the
      next page.
    """
    return _list_artifacts(
        stub=self._experiment_state_stub,
        parent_resource_name=self.resource_name,
        filter_query=filter_query,
        strongly_consistent=strongly_consistent,
        page_size=page_size,
        page_token=page_token,
    )

  # Other utility methods.

  def _create_experiment_unit(
      self,
      args: Mapping[str, Any] | None,
      role: xm.core.ExperimentUnitRole,
      identity: str,
  ) -> Awaitable[xm.ExperimentUnit]:
    """Creates a new WorkUnit instance for the experiment."""
    if not isinstance(role, WorkUnitSettings):
      raise ValueError(
          'Only WorkUnitSettings values for "role" are supported, got'
          f' {type(role).__name__}.'
      )
    settings = cast(WorkUnitSettings, role)
    work_unit = XManagerCloudWorkUnit(
        experiment=self,
        create_task=self._create_task,
        work_unit_id_predictor=self._id_predictor,
        experiment_state_stub=self._experiment_state_stub,
        identity=identity,
        args=args,
        settings=settings,
    )

    async def _get_work_unit():
      return work_unit

    # Need to return an Awaitable here to match the signature of the base class,
    # but we are not actually doing work unit creation asynchronously.
    return _get_work_unit()

  def _get_experiment_unit(
      self,
      experiment_id: int,
      identity: str,
      role: xm.core.ExperimentUnitRole,
      args: Mapping[str, Any] | None = None,
  ):
    raise NotImplementedError

  def _should_reload_experiment_unit(
      self, role: xm.core.ExperimentUnitRole
  ) -> bool:
    return False

  def _push_repo_state(self) -> None:
    """Pushes a snapshot of experiment code to the current git repo, if any."""

    result = snapshot_code.push_repo_state(self.id)
    user_command = snapshot_code.find_user_command()
    if result.commit_url is not None:
      self.create_artifact(
          url=url_pb2.Url(
              url=result.commit_url,
              display_name='Source code snapshot',
              icon='code',
          ),
          data_type=artifact_pb2.Artifact.DataType.CODE_SOURCE,
          lifecycle_phase=artifact_pb2.Artifact.LifecyclePhase.INPUT,
          additional_info=artifact_pb2.Artifact.AdditionalInfo(
              title='User launch command',
              code_block=' '.join(user_command),
          ),
      )
    launch_script_snapshot = snapshot_code.snapshot_launch_script(result)
    self.create_artifact(
        url=url_pb2.Url(
            url=launch_script_snapshot.launch_script_url,
            display_name='Launch script',
            icon='code',
        ),
        data_type=artifact_pb2.Artifact.DataType.CODE_SOURCE,
        lifecycle_phase=artifact_pb2.Artifact.LifecyclePhase.INPUT,
        additional_info=artifact_pb2.Artifact.AdditionalInfo(
            title=launch_script_snapshot.launch_script_path,
            code_block=launch_script_snapshot.launch_script_content,
        ),
    )

  def list_status_messages(
      self,
      *,
      filter_query: str | None = None,
      strongly_consistent: bool = False,
      page_size: int = 100,
      page_token: str | None = None,
  ) -> tuple[Sequence[status_message_pb2.StatusMessage], str | None]:
    """Lists status messages for the experiment.

    Args:
      filter_query: The filter query to use for the list operation.
      strongly_consistent: Whether to use strongly consistent reads for the list
        operation. True -> Strongly consistent reads, but higher latency and
        more expensive call. False -> Eventually consistent reads (<15s
        staleness), lower latency and database load.
      page_size: The page size to use for the list operation.
      page_token: The page token to use for the list operation.

    Returns:
      A tuple (messages, next_page_token), where messages is a list of
      status messages and next_page_token is the token for the next page.
    """
    return _list_status_messages(
        stub=self._experiment_state_stub,
        parent_resource_name=self.resource_name,
        filter_query=filter_query,
        strongly_consistent=strongly_consistent,
        page_size=page_size,
        page_token=page_token,
    )

  def _print_experiment_link(self) -> None:
    print()
    print(f'Launched experiment {self.id} {self.title!r}.')
    if ui_url := _get_xmanager_ui_url():
      print()
      print(f'XManager UI: {ui_url}/{self.resource_name}')

  def _update_experiment_launch_state(
      self,
      successful_launch: bool,
  ) -> None:
    if successful_launch:
      launch_state = status_pb2.ExperimentStatus.LaunchState.COMPLETED
    else:
      launch_state = status_pb2.ExperimentStatus.LaunchState.FAILED
    self._experiment_state_stub.update_experiment_launch_state(
        experiment_state_service_pb2.UpdateExperimentLaunchStateRequest(
            name=self.resource_name,
            launch_state=launch_state,
        )
    )

  def __exit__(self, exc_type, exc_value, traceback):
    self._print_experiment_link()
    self._update_experiment_launch_state(exc_type is None)
    super().__exit__(exc_type, exc_value, traceback)

  async def __aexit__(self, exc_type, exc_value, traceback):
    self._print_experiment_link()
    self._update_experiment_launch_state(exc_type is None)
    await super().__aexit__(exc_type, exc_value, traceback)


def _generate_create_experiment_request(
    experiment_title: str,
    # TODO: - Remove/update default value (e.g. to just the author).
    acls: xid_messages_pb2.XidAcls | None = xid_messages_pb2.XidAcls(
        owners=[xid_messages_pb2.XidAcls.Role(all_authenticated_users=True)]
    ),
    priority: priority_pb2.Priority = priority_pb2.Priority.NORMAL,
    urls: list[url_pb2.Url] | None = None,
    tags: list[str] | None = None,
    notes: str | None = None,
) -> experiment_state_service_pb2.CreateExperimentRequest:
  """Generates a CreateExperimentRequest for the Experiment State Server."""
  return experiment_state_service_pb2.CreateExperimentRequest(
      experiment=experiment_pb2.Experiment(
          title=experiment_title,
          acls=acls,
          priority=priority,
          urls=urls,
          tags=tags,
          notes=notes,
      )
  )


@functools.lru_cache(maxsize=1)
def _print_welcome_message() -> None:
  """Prints welcome message once per session."""
  print(termcolor.colored(_WELCOME_MESSAGE, color='green', attrs=['bold']))


def create_experiment(
    experiment_title: str | None = None,
    acls: xid_messages_pb2.XidAcls | None = None,
    priority: priority_pb2.Priority | None = None,
    urls: list[url_pb2.Url] | None = None,
    tags: list[str] | None = None,
    notes: str | None = None,
) -> XManagerCloudExperiment:
  """Creates a new XManager on Cloud experiment."""
  _print_welcome_message()
  stub = experiment_state_api.get_experiment_state_api()
  experiment_proto = stub.create_experiment(
      request=_generate_create_experiment_request(
          experiment_title=experiment_title,
          acls=acls,
          priority=priority,
          urls=urls,
          tags=tags,
          notes=notes,
      )
  )
  experiment = XManagerCloudExperiment(
      stub, experiment_proto, id_predictor=xm_id_predictor.Predictor(1)
  )
  try:
    experiment._push_repo_state()  # pylint: disable=protected-access
  except Exception:  # pylint: disable=broad-except
    logging.warning(
        'Failed to push repo state for experiment %d.',
        experiment.id,
        exc_info=True,
    )
  return experiment


def _parse_work_unit_name_from_operation(
    operation: operations_pb2.Operation,
) -> str:
  """Parse the work unit name from the operation."""
  # The operation name is of the form
  # `experiments/{XID}/workUnits/{WID}/operations/{operation_id}`, so we just
  # need to remove the /operations/{operation_id} suffix.
  return operation.name.split('/operations')[0]


def _convert_xm_job_to_ess_job(
    job: xm.Job,
    args: Any,
) -> work_unit_pb2.KubernetesJob:
  """Converts an XM Job to an ESS KubernetesJob."""
  if not isinstance(job.executor, xm_cloud_executor.KubernetesJobExecutor):
    raise ValueError(
        'Only KubernetesJobExecutor is supported, got'
        f' {type(job.executor).__name__}'
    )
  # We need to cast here to be able to use the `requirements` field below.
  executor = cast(xm_cloud_executor.KubernetesJobExecutor, job.executor)

  # Validate and convert resource requirements.
  requirements = executor.requirements.task_requirements
  error_message = (
      'Exactly one of (CPU and Memory), GPU, or TPU must be specified, got'
      f' {requirements}'
  )
  if len(requirements) > 2:
    raise ValueError(error_message)
  if len(requirements) == 2:
    if (
        xm.ResourceType.CPU in requirements
        and xm.ResourceType.MEMORY in requirements
    ):
      resources = resources_pb2.ResourceSet(
          cpu_resources=resources_pb2.CpuResources(
              cpu_millicores=int(requirements[xm.ResourceType.CPU] * 1000),
              memory_bytes=int(requirements[xm.ResourceType.MEMORY]),
          ),
      )
    else:
      raise ValueError(error_message)
  else:
    ((resource_type, count),) = requirements.items()
    if resource_type in xm.resources.GpuType:
      resources = resources_pb2.ResourceSet(
          gpu_resources=resources_pb2.GpuResources(
              type=str(resource_type),
              count=int(count),
          ),
      )
    elif resource_type in xm.resources.TpuType:
      topology = executor.requirements.topology
      if topology is None:
        raise ValueError(f'TPU resource {resource_type!r} requires a topology')
      resources = resources_pb2.ResourceSet(
          tpu_resources=resources_pb2.TpuResources(
              type=_tpu_type_to_gke_accelerator(resource_type, topology),
              topology=topology.name,
          ),
      )
    else:
      raise ValueError(error_message)

  if not isinstance(job.executable, executables.GoogleContainerRegistryImage):
    raise ValueError(
        'Only GoogleContainerRegistryImage executables are supported, got'
        f' {type(job.executable).__name__}. This should not be the case,'
        ' please check that you are using XMC packaging.'
    )

  dependency_order = None
  if executor.dependency:
    dependency_order = work_unit_pb2.KubernetesJob.DependencyOrder(
        job_name=executor.dependency.job_name,
        job_dependency_state=work_unit_pb2.KubernetesJob.DependencyOrder.JobDependencyState.Value(
            executor.dependency.state.value
        ),
    )

  return work_unit_pb2.KubernetesJob(
      name=job.name,
      spec=work_unit_pb2.ExecutableSpec(
          resources=resources,
          docker_image=job.executable.image_path,
          args=args,
          env_vars=job.env_vars,
      ),
      annotations=executor.annotations,
      replicas=executor.requirements.replicas,
      retry_limit=executor.replica_retry_limit,
      active_deadline_seconds=executor.active_deadline_seconds,
      dependency_order=dependency_order,
  )


def _collect_args_for_job(
    job: xm.Job, args_view: Mapping[str, Any]
) -> Mapping[str, Any]:
  """Collects the args for a job from the job itself and the job group args."""
  return {
      **job.args.to_dict(),
      **args_view.get(job.name, {}),
  }


def _convert_job_group_to_ess_job_set(
    job_group: xm.JobGroup,
    args_view: Mapping[str, Any],
) -> list[work_unit_pb2.KubernetesJob]:
  """Converts a JobGroup to a list of ESS KubernetesJobs."""
  return [
      _convert_xm_job_to_ess_job(job, _collect_args_for_job(job, args_view))
      for job in job_group.jobs.values()
  ]


class XManagerCloudWorkUnit(xm.WorkUnit):
  """A work unit running on the XManager on Cloud backend."""

  def __init__(
      self,
      experiment: XManagerCloudExperiment,
      create_task: Callable[[Awaitable[Any]], futures.Future[Any]],
      experiment_state_stub: experiment_state_api.ExperimentStateGRpcApi,
      settings: WorkUnitSettings,
      work_unit_id_predictor: xm_id_predictor.Predictor | None = None,
      identity: str | None = None,
      args: Mapping[str, Any] | None = None,
      work_unit_proto: work_unit_pb2.WorkUnit | None = None,
  ):
    super().__init__(
        experiment=experiment,
        create_task=create_task,
        args=args,
        role=settings,
    )
    self._experiment = experiment
    self._experiment_state_stub = experiment_state_stub
    self._work_unit_id_predictor = work_unit_id_predictor
    self._identity = identity
    self._args = args
    self._settings = settings
    self._work_unit_proto = work_unit_proto

    if work_unit_proto and work_unit_proto.name:
      self._resource_name = work_unit_proto.name
      self._work_unit_id = self._parse_work_unit_id(self._resource_name)
    elif self._work_unit_id_predictor is not None:
      self._work_unit_id = self._work_unit_id_predictor.reserve_id()
      self._resource_name = (
          f'experiments/{self._experiment.id}/workUnits/{self._work_unit_id}'
      )
    # If we do not have a work unit ID predictor, an ID will be generated when
    # the work unit is created in the backend.
    else:
      self._work_unit_id = None
      self._resource_name = None

  @property
  def work_unit_id(self) -> int:
    """The unique ID of the work unit."""
    if self._work_unit_id is not None:
      return self._work_unit_id
    else:
      raise RuntimeError(
          'The work unit ID is not known. This might be because the work unit '
          'was not loaded correctly from the backend proto.'
      )

  @property
  def experiment_unit_name(self) -> str:
    """The resource name of the work unit (i.e.

    experiments/{XID}/workUnits/{WID}).

    Raises:
      RuntimeError: If the work unit has not been created in the XM backend yet.
    """
    if self._resource_name is None:
      raise RuntimeError(
          'The work unit has not been created in the XM backend yet, so the'
          ' resource name is not yet known.'
      )
    return self._resource_name

  @property
  def work_unit_proto(self) -> work_unit_pb2.WorkUnit | None:
    """The proto of the work unit."""
    return self._work_unit_proto

  def _update_work_unit(
      self,
      *,
      priority: priority_pb2.Priority | None = None,
      urls: Sequence[url_pb2.Url] | None = None,
      tags: Sequence[str] | None = None,
      notes: str | None = None,
  ) -> None:
    """Updates the work unit."""
    kwargs = {
        'priority': priority,
        'urls': urls,
        'tags': tags,
        'notes': notes,
    }
    paths = [name for name, value in kwargs.items() if value is not None]
    updated_proto = self._experiment_state_stub.update_work_unit(
        experiment_state_service_pb2.UpdateWorkUnitRequest(
            work_unit=work_unit_pb2.WorkUnit(
                name=self._resource_name,
                **kwargs,
            ),
            update_mask=field_mask_pb2.FieldMask(paths=paths),
        )
    )
    self._work_unit_proto = updated_proto

  def set_priority(
      self,
      priority: priority_pb2.Priority,
  ) -> None:
    """Sets the priority of the work unit."""
    self._update_work_unit(priority=priority)

  def set_notes(self, notes: str) -> None:
    """Sets the notes of the work unit."""
    self._update_work_unit(notes=notes)

  # Methods for updating tags.

  def set_tags(self, tags: Sequence[str]) -> None:
    """Sets the tags of the work unit.

    Overwrites any existing tags.

    Args:
      tags: The tags to set for the work unit.
    """
    self._update_work_unit(tags=tags)

  def add_tags(self, tags: Sequence[str]) -> None:
    """Adds the tags to the work unit.

    Appends to existing tags.

    Args:
      tags: The tags to add to the list of tags for the work unit.
    """
    existing_tags = self._settings.tags
    self._update_work_unit(tags=list(itertools.chain(existing_tags, tags)))

  def remove_tags(self, tags_to_remove: Sequence[str]) -> None:
    """Removes specified tags from the work unit, if present."""
    existing_tags = self._settings.tags
    self._update_work_unit(
        tags=[tag for tag in existing_tags if tag not in tags_to_remove],
    )

  # Methods for updating URLs.

  def set_urls(self, urls: Sequence[url_pb2.Url]) -> None:
    """Sets the URLs to be displayed for the work unit.

    Overwrites any existing URLs.

    Args:
      urls: The URLs to set for the work unit.
    """
    self._update_work_unit(urls=urls)

  def add_urls(self, urls: Sequence[url_pb2.Url]) -> None:
    """Adds the URLs to the work unit.

    Appends to existing URLs.

    Args:
      urls: The URLs to add to the list of URLs for the work unit.
    """
    existing_urls = self._settings.urls
    self._update_work_unit(urls=list(itertools.chain(existing_urls, urls)))

  def remove_urls(self, urls_to_remove: Sequence[url_pb2.Url]) -> None:
    """Removes URLs from the work unit, if present."""
    existing_urls = self._settings.urls
    self._update_work_unit(
        urls=[url for url in existing_urls if url not in urls_to_remove],
    )

  # Methods related to artifacts.

  def create_artifact(self, **kwargs) -> Artifact:
    """Creates an artifact for the work unit.

    Args:
      **kwargs: Keyword arguments defining the artifact. Similar to
        Experiment.create_artifact.

    Returns:
      The created Artifact wrapper.
    """
    return _create_artifact(
        stub=self._experiment_state_stub,
        parent_resource_name=self.experiment_unit_name,
        **kwargs,
    )

  def list_artifacts(
      self,
      *,
      filter_query: str | None = None,
      strongly_consistent: bool = False,
      page_size: int = 100,
      page_token: str | None = None,
  ) -> tuple[Sequence[artifact_pb2.Artifact], str | None]:
    """Lists artifacts for the work unit.

    Args:
      filter_query: The filter query to use for the list operation.
      strongly_consistent: Whether to use strongly consistent reads for the list
        operation. True -> Strongly consistent reads, but higher latency and
        more expensive call. False -> Eventually consistent reads (<15s
        staleness), lower latency and database load.
      page_size: The page size to use for the list operation.
      page_token: The page token to use for the list operation.

    Returns:
      A tuple (artifacts, next_page_token), where artifacts is a list of
      artifacts for the work unit and next_page_token is the token for the
      next page.
    """
    return _list_artifacts(
        stub=self._experiment_state_stub,
        parent_resource_name=self.experiment_unit_name,
        filter_query=filter_query,
        strongly_consistent=strongly_consistent,
        page_size=page_size,
        page_token=page_token,
    )

  # Other utility methods.

  async def _launch_job_group(
      self,
      job_group: xm.JobGroup,
      args_view: Mapping[str, Any],
      identity: str,  # pylint: disable=unused-argument
  ) -> None:
    """Launches a job group for the work unit."""
    # Create a work unit in the XManager backend.
    # TODO: - call WaitOperation once implemented and get the full
    # work unit or handle errors.
    self._work_unit_operation = self._experiment_state_stub.create_work_unit(
        experiment_state_service_pb2.CreateWorkUnitRequest(
            parent=self._experiment.resource_name,
            work_unit_id=self._work_unit_id,
            work_unit=work_unit_pb2.WorkUnit(
                executable=work_unit_pb2.Executable(
                    scheduling_constraints=self._settings.scheduling_constraints,
                    jobset=work_unit_pb2.JobSetExecutable(
                        jobs=_convert_job_group_to_ess_job_set(
                            job_group, args_view
                        )
                    ),
                ),
                priority=self._settings.priority
                if self._settings.priority is not None
                else self._experiment.priority,
                work_unit_type=self._settings.work_unit_type,
                tags=self._settings.tags,
                notes=self._settings.notes,
                urls=self._settings.urls,
            ),
        )
    )
    self._resource_name = _parse_work_unit_name_from_operation(
        self._work_unit_operation
    )
    self._work_unit_id = self._parse_work_unit_id(self._resource_name)

  def list_status_messages(
      self,
      *,
      filter_query: str | None = None,
      strongly_consistent: bool = False,
      page_size: int = 100,
      page_token: str | None = None,
  ) -> tuple[Sequence[status_message_pb2.StatusMessage], str | None]:
    """Lists status messages for the work unit.

    Args:
      filter_query: The filter query to use for the list operation.
      strongly_consistent: Whether to use strongly consistent reads for the list
        operation. True -> Strongly consistent reads, but higher latency and
        more expensive call. False -> Eventually consistent reads (<15s
        staleness), lower latency and database load.
      page_size: The page size to use for the list operation.
      page_token: The page token to use for the list operation.

    Returns:
      A tuple (messages, next_page_token), where messages is a list of
      status messages and next_page_token is the token for the next page.
    """
    return _list_status_messages(
        stub=self._experiment_state_stub,
        parent_resource_name=self.experiment_unit_name,
        filter_query=filter_query,
        strongly_consistent=strongly_consistent,
        page_size=page_size,
        page_token=page_token,
    )

  def _parse_work_unit_id(self, work_unit_name: str) -> int:
    """Parse the work unit ID from the resource name.

    Args:
      work_unit_name: The resource name of the work unit (i.e.
        experiments/{XID}/workUnits/{WID}).

    Returns:
      The work unit ID (i.e. {WID} in the example above).
    """
    return int(work_unit_name.split('/')[-1])


def _get_settings_from_work_unit_proto(
    work_unit_proto: work_unit_pb2.WorkUnit,
) -> WorkUnitSettings:
  """Returns the settings for the work unit."""
  return WorkUnitSettings(
      work_unit_type=work_unit_proto.work_unit_type,
      scheduling_constraints=work_unit_proto.executable.scheduling_constraints,
      priority=work_unit_proto.priority,
      tags=work_unit_proto.tags,
      notes=work_unit_proto.notes,
      urls=work_unit_proto.urls,
  )


def _convert_ess_proto_to_xm_cloud_experiment(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    experiment_proto: experiment_pb2.Experiment,
) -> XManagerCloudExperiment:
  """Returns the experiment for the given proto."""
  return XManagerCloudExperiment(
      experiment_state_stub=stub,
      experiment_proto=experiment_proto,
  )


def _convert_ess_proto_to_xm_cloud_work_unit(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    experiment_proto: experiment_pb2.Experiment,
    work_unit_proto: work_unit_pb2.WorkUnit,
) -> XManagerCloudWorkUnit:
  """Returns the work unit for the given protos."""
  experiment = _convert_ess_proto_to_xm_cloud_experiment(stub, experiment_proto)
  return XManagerCloudWorkUnit(
      experiment=experiment,
      create_task=experiment._create_task,  # pylint: disable=protected-access
      experiment_state_stub=stub,
      work_unit_id_predictor=experiment._id_predictor,  # pylint: disable=protected-access
      settings=_get_settings_from_work_unit_proto(work_unit_proto),
      identity=None,
      args=None,
      work_unit_proto=work_unit_proto,
  )


def get_experiment(
    experiment_id: int,
) -> XManagerCloudExperiment:
  """Returns the experiment with the given ID."""
  stub = experiment_state_api.get_experiment_state_api()
  return _convert_ess_proto_to_xm_cloud_experiment(
      stub,
      stub.get_experiment(
          experiment_state_service_pb2.GetExperimentRequest(
              name=f'experiments/{experiment_id}'
          )
      ),
  )


def get_work_unit(
    experiment_id: int, work_unit_id: int
) -> XManagerCloudWorkUnit:
  """Returns the work unit with the given ID."""
  stub = experiment_state_api.get_experiment_state_api()
  experiment = get_experiment(experiment_id)
  work_unit_proto = stub.get_work_unit(
      experiment_state_service_pb2.GetWorkUnitRequest(
          name=f'experiments/{experiment_id}/workUnits/{work_unit_id}'
      )
  )
  return _convert_ess_proto_to_xm_cloud_work_unit(
      stub, experiment._experiment_proto, work_unit_proto  # pylint: disable=protected-access
  )


def get_current_experiment() -> XManagerCloudExperiment:
  """Returns the current experiment.

  Usually it is the one which the current binary belongs to. But if called
  within `with Experiment` block (e.g. in a launch script) or from a
  JobGenerator, returns the experiment which is being launched.

  Returns: The current experiment, if it is an xm_cloud.XManagerCloudExperiment.

  Raises:
    RuntimeError: If no current experiment found or it is not an
      xm_cloud.XManagerCloudExperiment.
  """
  current_experiment = xm.core._current_experiment.get(None)  # pylint: disable=protected-access
  if current_experiment:
    if not isinstance(current_experiment, XManagerCloudExperiment):
      raise RuntimeError(
          'Current experiment is not an XManagerCloudExperiment.'
      )
    # Retrieve the up to date experiment proto from the backend.
    return get_experiment(current_experiment.id)
  raise RuntimeError('No current experiment found.')


def get_current_work_unit() -> XManagerCloudWorkUnit:
  """Returns the current work unit, if it is an xm_cloud.XManagerCloudWorkUnit."""
  current_work_unit = xm.core._current_experiment_unit.get(None)  # pylint: disable=protected-access
  if current_work_unit:
    if not isinstance(current_work_unit, XManagerCloudWorkUnit):
      raise RuntimeError('Current work unit is not an XManagerCloudWorkUnit.')
    # Retrieve the up to date work unit proto from the backend.
    return get_work_unit(
        current_work_unit._experiment.id, current_work_unit.work_unit_id  # pylint: disable=protected-access
    )
  raise RuntimeError('No current work unit found.')


def list_experiments(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    *,
    filter_query: str | None = None,
    strongly_consistent: bool = False,
    page_size: int = 100,
    page_token: str | None = None,
) -> tuple[Sequence[XManagerCloudExperiment], str | None]:
  """Lists experiments.

  Args:
    stub: The experiment state stub.
    filter_query: The filter query to use for the list operation.
    strongly_consistent: Whether to use strongly consistent reads for the list
      operation. True -> Strongly consistent reads, but higher latency and more
      expensive call. False -> Eventually consistent reads (<15s staleness),
      lower latency and database load.
    page_size: The page size to use for the list operation.
    page_token: The page token to use for the list operation.

  Returns:
    A tuple (experiments, next_page_token), where experiments is a list of
    experiments and next_page_token is the token for the next page.
  """
  request = experiment_state_service_pb2.ListExperimentsRequest(
      filter=filter_query,
      page_token=page_token,
      page_size=page_size,
  )
  if strongly_consistent:
    response = stub.list_experiments(request)
  else:
    response = stub.search_experiments(request)

  experiments = [
      _convert_ess_proto_to_xm_cloud_experiment(stub, experiment)
      for experiment in response.experiments
  ]
  return experiments, response.next_page_token


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


def _create_artifact(
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
        f'_create_artifact() got unexpected keyword arguments: {kwargs.keys()}'
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


def _list_artifacts(
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


def _list_status_messages(
    stub: experiment_state_api.ExperimentStateGRpcApi,
    parent_resource_name: str,
    *,
    filter_query: str | None = None,
    strongly_consistent: bool = False,
    page_size: int = 100,
    page_token: str | None = None,
) -> tuple[Sequence[status_message_pb2.StatusMessage], str | None]:
  """Lists status messages for the given parent resource.

  Args:
    stub: The experiment state stub.
    parent_resource_name: The resource name of the parent (either an Experiment
      or a WorkUnit). Format is one of the following: "experiments/{XID}"
      "experiments/{XID}/workUnits/{WID}"
    filter_query: The filter query to use for the list operation.
    strongly_consistent: Whether to use strongly consistent reads.
    page_size: The page size to use for the list operation.
    page_token: The page token to use for the list operation.

  Returns:
    A tuple (messages, next_page_token), where messages is a list of
    status messages and next_page_token is the token for the next page.
  """
  request = experiment_state_service_pb2.ListStatusMessagesRequest(
      parent=parent_resource_name,
      filter=filter_query,
      page_token=page_token,
      page_size=page_size,
  )
  if strongly_consistent:
    response = stub.list_status_messages(request)
  else:
    response = stub.search_status_messages(request)
  return list(response.status_messages), response.next_page_token


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
    *,
    url: url_pb2.Url | None = None,
    acls: xid_messages_pb2.XidAcls | None = None,
    tags: Sequence[str] | None = None,
    notes: str | None = None,
    lifecycle_phase: artifact_pb2.Artifact.LifecyclePhase | None = None,
    data_type: artifact_pb2.Artifact.DataType | None = None,
    mime_type: str | None = None,
    additional_info: artifact_pb2.Artifact.AdditionalInfo | None = None,
) -> artifact_pb2.Artifact:
  """Updates an artifact in the Experiment State Service database."""
  kwargs = {
      'url': url,
      'acls': acls,
      'tags': tags,
      'notes': notes,
      'lifecycle_phase': lifecycle_phase,
      'data_type': data_type,
      'mime_type': mime_type,
      'additional_info': additional_info,
  }
  paths = [name for name, value in kwargs.items() if value is not None]
  if not paths:
    raise ValueError('At least one field must be specified for update.')

  return stub.update_artifact(
      experiment_state_service_pb2.UpdateArtifactRequest(
          artifact=artifact_pb2.Artifact(
              name=artifact_resource_name,
              **kwargs,
          ),
          update_mask=field_mask_pb2.FieldMask(paths=paths),
      )
  )
