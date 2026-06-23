"""Restart command for XManager Cloud CLI."""

from collections.abc import Sequence

from absl import app

from google.longrunning import operations_pb2
from xmanager_cloud.experiment_state_server import experiment_state_api
from xmanager_cloud.experiment_state_server.proto import api_pb2 as experiment_state_service_pb2


def _restart_experiment(experiment_id: str) -> operations_pb2.Operation:
  """Restarts an experiment."""
  return (
      experiment_state_api.get_experiment_state_api().batch_restart_work_units(
          experiment_state_service_pb2.BatchRestartWorkUnitsRequest(
              parent=f'experiments/{experiment_id}',
          )
      )
  )


def restart_command(argv: Sequence[str]) -> None:
  if len(argv) < 3:
    raise app.UsageError('Please specify an experiment ID to restart.')
  if len(argv) > 3:
    raise app.UsageError(
        'Too many command-line arguments. Please specify a single experiment ID'
        ' to restart.'
    )
  experiment_id = argv[2]
  print(f'Restarting experiment {experiment_id}.')
  _restart_experiment(experiment_id)
  # TODO: - Add nicer logs once WaitOperation is implemented in the
  # backend.
