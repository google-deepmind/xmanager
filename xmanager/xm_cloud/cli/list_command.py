"""List command for XManager Cloud CLI."""

from collections.abc import Sequence

from absl import flags
import tabulate
import xmanager.xm_cloud as xmc

from xmanager_cloud.experiment_state_server import experiment_state_api
from xmanager_cloud.experiment_state_server.proto import status_pb2

_STRONGLY_CONSISTENT = flags.DEFINE_bool(
    'strongly_consistent',
    False,
    'Whether to use strongly consistent reads for the list operation. True ->'
    ' Strongly consistent reads, but higher latency and more expensive call.'
    ' False -> Eventually consistent reads (<15s staleness), lower latency and'
    ' database load.',
)

_FILTER_QUERY = flags.DEFINE_string(
    'filter_query',
    None,
    'Filter query to use for the list operation. AIP-160 compliant.',
)


def _get_failed_work_units_count(
    experiment: xmc.XManagerCloudExperiment,
) -> int:
  """Returns the number of failed work units for the experiment."""
  return sum(
      state_count.count
      for state_count in experiment.status.child_state_counts
      if state_count.state in (status_pb2.WorkUnitStatus.WorkUnitState.FAILED,)
  )


def _print_experiments(
    experiments: Sequence[xmc.XManagerCloudExperiment],
) -> None:
  """Prints experiments."""
  columns = {
      'XID': lambda experiment: experiment.id,
      'Title': lambda experiment: experiment.title,
      'Author': lambda experiment: experiment.author,
      'Creation Time': lambda experiment: experiment.creation_time,
      'Status': lambda experiment: status_pb2.ExperimentStatus.ActiveState.Name(
          experiment.status.active_state
      ),
      'Failed Work Units': _get_failed_work_units_count,
  }
  print(
      tabulate.tabulate(
          [
              [f(experiment) for f in columns.values()]
              for experiment in experiments
          ],
          headers=columns.keys(),
      )
  )


def list_command() -> None:
  """Lists experiments."""
  all_experiments = []
  next_page_token = None
  while True:
    experiments, next_page_token = xmc.list_experiments(
        stub=experiment_state_api.get_experiment_state_api(),
        filter_query=_FILTER_QUERY.value,
        strongly_consistent=_STRONGLY_CONSISTENT.value,
        page_token=next_page_token,
    )
    all_experiments.extend(experiments)
    if next_page_token is None:
      break
  if all_experiments:
    _print_experiments(all_experiments)
  else:
    print('No experiments found.')
