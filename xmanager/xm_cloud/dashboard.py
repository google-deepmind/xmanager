"""Dashboard, Chart, and Plot wrappers for XManager on Cloud."""

from collections.abc import Iterable, Mapping, Sequence
import functools
import os
from typing import Any

import grpc

from google.protobuf import field_mask_pb2


class _DummyProtoMessage:
  """Dummy proto message class for fallback when protos are not injected."""

  def __init__(self, *args: Any, **kwargs: Any):
    del args
    for k, v in kwargs.items():
      setattr(self, k, v)


class _DummyProto:
  """Dummy proto module for fallback when protos are not injected."""

  # pylint: disable=invalid-name
  CreateDashboardRequest: Any = _DummyProtoMessage
  GetDashboardRequest: Any = _DummyProtoMessage
  ListDashboardsRequest: Any = _DummyProtoMessage
  UpdateDashboardRequest: Any = _DummyProtoMessage
  DashboardServiceStub: Any = _DummyProtoMessage
  Dashboard: Any = _DummyProtoMessage
  Chart: Any = _DummyProtoMessage
  PlotSpec: Any = _DummyProtoMessage

  def __getattr__(self, name: str) -> Any:
    return _DummyProtoMessage


# Injected protobuf dependencies from github.com/google/xmc repo.
# Specifically from api/protos/dashboard. During local builds before
# present, so we fallback gracefully for analysis and testing.
try:
  from xmanager_cloud.dashboard_service.proto import dashboard_service_pb2 as api_pb2  # pylint: disable=g-import-not-at-top  # pyrefly: ignore[missing-import]
  from xmanager_cloud.dashboard_service.proto import dashboard_service_pb2_grpc as api_pb2_grpc  # pylint: disable=g-import-not-at-top  # pyrefly: ignore[missing-import]
  from xmanager_cloud.dashboard_service.proto import messages_pb2 as dashboard_pb2  # pylint: disable=g-import-not-at-top  # pyrefly: ignore[missing-import]
except ImportError:
  api_pb2 = _DummyProto()
  api_pb2_grpc = _DummyProto()
  dashboard_pb2 = _DummyProto()


@functools.lru_cache()
def get_dashboard_service_stub() -> Any:
  """Returns a cached gRPC stub for DashboardService.

  Uses an insecure channel for localhost/loopback endpoints and a secure SSL
  channel for remote endpoints.
  """
  endpoint = os.environ.get('XMANAGER_DASHBOARD_ENDPOINT', 'localhost:8080')
  if endpoint.startswith(('localhost', '127.0.0.1', '[::1]', '[::]')):
    channel = grpc.insecure_channel(endpoint)
  else:
    channel = grpc.secure_channel(endpoint, grpc.ssl_channel_credentials())
  return api_pb2_grpc.DashboardServiceStub(channel)


class Plot:
  """A plot visualization inside a chart."""

  def __init__(self, plot_proto: Any):
    self._plot_proto = plot_proto

  @property
  def title(self) -> str:
    """The title of the plot."""
    return getattr(self._plot_proto, 'title', '')

  def to_proto(self) -> Any:
    """Returns the underlying protobuf message."""
    return self._plot_proto


class Chart:
  """A chart inside a dashboard, referencing exactly 1 experiment and containing Plots."""

  def __init__(self, chart_proto: Any):
    self._chart_proto = chart_proto

  @property
  def title(self) -> str:
    """The title of the chart."""
    return getattr(self._chart_proto, 'title', '')

  @property
  def experiment_id(self) -> int:
    """The XID of the experiment referenced by this chart."""
    return getattr(self._chart_proto, 'xid', 0)

  @property
  def experiment_name(self) -> str:
    """The full resource name of the experiment referenced by this chart."""
    return f'experiments/{self.experiment_id}'

  @property
  def plots(self) -> Sequence[Plot]:
    """The list of plots contained in this chart."""
    plots_list = getattr(self._chart_proto, 'plots', [])
    return [Plot(plot_proto) for plot_proto in plots_list]

  def to_proto(self) -> Any:
    """Returns the underlying protobuf message."""
    return self._chart_proto


_VALID_DASHBOARD_FIELDS = {'name', 'title', 'charts'}
_REPEATED_DASHBOARD_FIELDS = {'charts'}


class Dashboard:
  """A standalone dashboard resource containing one or more Charts."""

  def __init__(
      self,
      dashboard_proto: Any,
      charts: Sequence[Chart] | None = None,
      stub: Any | None = None,
  ):
    self._dashboard_proto = dashboard_proto
    self._charts = list(charts) if charts is not None else None
    self._stub = stub or get_dashboard_service_stub()

  @property
  def name(self) -> str:
    """The standalone resource name of the dashboard (e.g. dashboards/123)."""
    return getattr(self._dashboard_proto, 'name', '')

  @property
  def id(self) -> str:
    """The unique ID of the dashboard."""
    return self.name.split('/')[-1] if self.name else ''

  @property
  def title(self) -> str:
    """The display title of the dashboard."""
    return getattr(self._dashboard_proto, 'title', '')

  @property
  def charts(self) -> Sequence[Chart]:
    """The list of charts contained in this dashboard."""
    if self._charts is not None:
      return self._charts
    charts_list = getattr(self._dashboard_proto, 'charts', [])
    return [Chart(chart_proto) for chart_proto in charts_list]

  def update(self, **kwargs: Any) -> None:
    """Updates fields on the dashboard and saves via gRPC.

    An AIP-134 FieldMask is automatically populated in UpdateDashboardRequest
    containing the keys passed in kwargs.

    Args:
      **kwargs: Fields on the Dashboard protobuf message to update.

    Raises:
      ValueError: If an unknown field is passed in kwargs.
      TypeError: If a repeated field is passed a non-iterable value.
    """
    fields_by_name = getattr(
        getattr(self._dashboard_proto, 'DESCRIPTOR', None),
        'fields_by_name',
        None,
    )
    for key, value in kwargs.items():
      if hasattr(value, 'to_proto'):
        value = value.to_proto()
      elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        value = [v.to_proto() if hasattr(v, 'to_proto') else v for v in value]

      is_repeated = False
      if isinstance(fields_by_name, Mapping):
        if key not in fields_by_name:
          raise ValueError(f'Unknown field {key!r} for Dashboard.')
        field_desc = fields_by_name[key]
        if getattr(field_desc, 'label', None) == getattr(
            field_desc, 'LABEL_REPEATED', -1
        ):
          is_repeated = True
      else:
        if key not in _VALID_DASHBOARD_FIELDS:
          raise ValueError(f'Unknown field {key!r} for Dashboard.')
        if key in _REPEATED_DASHBOARD_FIELDS:
          is_repeated = True

      if is_repeated:
        if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
          raise TypeError(
              f'Field {key!r} is repeated and expects an iterable, got'
              f' {type(value).__name__}.'
          )
        if not hasattr(self._dashboard_proto, key):
          setattr(self._dashboard_proto, key, list(value))
        else:
          repeated_field = getattr(self._dashboard_proto, key)
          del repeated_field[:]
          repeated_field.extend(value)
      else:
        setattr(self._dashboard_proto, key, value)

    request = api_pb2.UpdateDashboardRequest(
        dashboard=self._dashboard_proto,
        update_mask=field_mask_pb2.FieldMask(paths=list(kwargs.keys())),
    )
    self._dashboard_proto = self._stub.UpdateDashboard(request)


def create_plot(title: str, plot_id: str | None = None, **kwargs: Any) -> Plot:
  """Creates a new Plot wrapper.

  Args:
    title: Display title for the plot.
    plot_id: Optional unique identifier for the plot within a chart. If not
      provided, defaults to a slug derived from title.
    **kwargs: Additional fields passed directly to the PlotSpec protobuf.

  Returns:
    The created Plot wrapper instance.
  """
  plot_id = plot_id or title.lower().replace(' ', '_')
  proto = dashboard_pb2.PlotSpec(title=title, plot_id=plot_id, **kwargs)
  return Plot(proto)


def create_chart(
    title: str,
    experiment_id: int,
    plots: Sequence[Plot | Any],
    **kwargs: Any,
) -> Chart:
  """Creates a new Chart wrapper referencing exactly 1 experiment.

  Args:
    title: Display title for the chart.
    experiment_id: XID of the experiment referenced by this chart.
    plots: Sequence of plots to include in the chart.
    **kwargs: Additional fields passed directly to the Chart protobuf message
      (such as description or plot_layout).

  Returns:
    The created Chart wrapper instance.

  Raises:
    ValueError: If plots is empty.
  """
  if not plots:
    raise ValueError('A Chart must contain at least one Plot.')
  plot_protos = [p.to_proto() if isinstance(p, Plot) else p for p in plots]
  proto = dashboard_pb2.Chart(
      title=title,
      xid=experiment_id,
      plots=plot_protos,
      **kwargs,
  )
  return Chart(proto)


def create_dashboard(
    title: str,
    charts: Sequence[Chart | Any],
    **kwargs: Any,
) -> Dashboard:
  """Creates a new standalone dashboard resource.

  Args:
    title: Display title for the dashboard.
    charts: One or more charts to include in the dashboard. Each chart must
      reference exactly 1 experiment.
    **kwargs: Additional fields passed directly to the Dashboard protobuf
      message (such as description or chart_layout).

  Returns:
    The created Dashboard wrapper instance.

  Raises:
    ValueError: If charts is empty.
  """
  if not charts:
    raise ValueError('A dashboard must contain at least one Chart.')

  chart_protos = [c.to_proto() if isinstance(c, Chart) else c for c in charts]
  proto = dashboard_pb2.Dashboard(title=title, **kwargs)
  request = api_pb2.CreateDashboardRequest(dashboard=proto, charts=chart_protos)

  stub = get_dashboard_service_stub()
  created_proto = stub.CreateDashboard(request)
  chart_wrappers = [c if isinstance(c, Chart) else Chart(c) for c in charts]
  return Dashboard(created_proto, charts=chart_wrappers, stub=stub)


def get_dashboard(name: str) -> Dashboard:
  """Retrieves an existing dashboard by its resource name (e.g. dashboards/123).

  Args:
    name: The standalone resource name of the dashboard.

  Returns:
    The retrieved Dashboard wrapper instance.
  """
  stub = get_dashboard_service_stub()
  request = api_pb2.GetDashboardRequest(name=name)
  return Dashboard(stub.GetDashboard(request), stub=stub)


def list_dashboards(*, filter_query: str | None = None) -> Sequence[Dashboard]:
  """Lists dashboards, optionally filtered by AIP-132 filter query.

  Args:
    filter_query: Optional AIP-132 filter query string.

  Returns:
    A sequence of matching Dashboard wrapper instances.
  """
  stub = get_dashboard_service_stub()
  request = api_pb2.ListDashboardsRequest(filter=filter_query or '')
  response = stub.ListDashboards(request)
  return [Dashboard(proto, stub=stub) for proto in response.dashboards]
