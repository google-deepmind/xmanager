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
"""Helper methods for running Tensorboard from the client."""

from typing import Any, Mapping, Optional

from xmanager import xm


class TensorboardProvider:
  """A class to generate package and job/args to Tensorboard jobs."""

  DEFAULT_TENSORBOARD_PORT = 6006

  @staticmethod
  def get_tensorboard_packageable(timeout_secs: int) -> xm.PythonContainer:
    """Creates container spec running TensorBoard server.

    Args:
      timeout_secs: Seconds to run the server for. Note that a value of 0
        disables the associated timeout.

    Raises:
      RuntimeError: `timeout_secs` is negative.

    Returns:
      Spec of container running TensorBoard server for a specified
      period of time.
    """
    if timeout_secs < 0:
      raise RuntimeError('`timeout_secs` must be a nonnegative number')

    return xm.PythonContainer(
        base_image='tensorflow/tensorflow',
        entrypoint=xm.CommandList([f'timeout {timeout_secs}s tensorboard']),
    )

  @staticmethod
  def get_tensorboard_job_args(
      log_dir: str,
      port: Optional[int] = None,
      additional_args: Optional[Mapping[str, Any]] = None,
  ) -> Mapping[str, Any]:
    """Get arguments to start a Tensorboard job."""
    args = {
        'logdir': log_dir,
        'port': port or TensorboardProvider.DEFAULT_TENSORBOARD_PORT,
        # Allows accessing visualisations from Docker container running locally.
        'host': '0.0.0.0',
        # This is set to true by default when running Tensorboard.
        # Since it doesn't seem to be working well with GKE Workload Identity,
        # we set it to false for now. Can be overriden through the
        # `additional_args` param.
        #
        # https://github.com/tensorflow/tensorboard/issues/4784#issuecomment-868945650
        'load_fast': 'false',
    }
    if additional_args:
      args.update(additional_args)

    return args


def add_tensorboard(
    experiment: xm.Experiment,
    logdir: str,
    executor: xm.Executor,
    timeout_secs: int = 60 * 60 * 24,
    args: Optional[Mapping[str, Any]] = None,
) -> None:
  """Self-contained function which adds a Tensorboard auxiliary job to @experiment."""
  provider = TensorboardProvider
  [executable] = experiment.package(
      [
          xm.Packageable(
              provider.get_tensorboard_packageable(timeout_secs=timeout_secs),
              executor.Spec(),
          )
      ]
  )

  job = xm.Job(
      executable,
      executor,
      args=provider.get_tensorboard_job_args(logdir, additional_args=args),
      name='tensorboard',
  )

  # TODO: Add support for `termination_delay_secs`.
  experiment.add(xm.AuxiliaryUnitJob(job, termination_delay_secs=0))
