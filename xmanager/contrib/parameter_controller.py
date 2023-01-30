# Copyright 2021 DeepMind Technologies Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a use of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tools to write parameter controllers for XManager experiments.

An async function decorated with @parameter_controller.controller() would return
an xm.JobGenerator. When added to an experiment, an AUX unit would be started
which would run the given function long-term.

Usage:
  from xmanager.contrib import parameter_controller

  @parameter_controller.controller(...)
  async def my_controller(foo, bar, *, experiment: xm.Experiment) -> None:
    experiment.add(...)
  ...
  experiment.add(my_controller(foo=1, bar=2))
"""
import asyncio
import json
import os
import shutil
from typing import Any, Callable, Dict, Optional

from absl import flags
import launchpad as lp
import launchpad.nodes.python.xm_docker as lp_docker
from xmanager import xm
from xmanager import xm_local


def _parameter_controller_job_args(
    controller_args: Dict[str, Any]
) -> Dict[str, Any]:
  """Converts given XM flags to coresponding Launchpad's `process_entry` flags.

  The XM client runs inside `process_entry`, but flags are not defined yet.
  The `flags_to_populate` flag in `process_entry` is used to define the
  given flags before calling `app.run()`. This function converts XM flags
  to the format desired by `process_entry`.

  Args:
    controller_args: XM flags to be passed.

  Returns:
    Flags used to populate XM flags in `process_entry`
  """
  args = {
      'lp_task_id': 0,
      'flags_to_populate': xm.ShellSafeArg(json.dumps(controller_args)),
  }

  return args


def _to_python_container(
    node: lp.PyNode, label: str, docker_config: lp.DockerConfig
) -> xm.PythonContainer:
  """Returns xm.PythonContainer embedding a lp.PyNode."""
  return lp_docker.to_docker_executables(
      [node], label=label, docker_config=docker_config
  )[0][0]


def _use_host_db_config(
    package_path: str,
    controller_args: Dict[str, Any],
) -> None:
  """Make DB config file used by host available to controller job.

  Involves copying the DB config file used by host to the package directory of
  the container and then setting the `--xm_db_yaml_config_path` accordingly.

  Args:
    package_path: The path of the package directory where the DB config should
      be copied.
    controller_args: The flag mapping used by the controller job. Used to update
      `--xm_db_yaml_config_path`.

  Raises:
    RuntimeError: The `--xm_db_yaml_config` flag will be overriden by this
      function. To avoid confusion about this behavior, an exception is thrown
      if it's already set.
  """

  if 'xm_db_yaml_config_path' in controller_args:
    raise RuntimeError(
        "Parameter controller can't use host DB config "
        'and also use `--xm_db_yaml_config_path` flag. Use '
        '`use_host_db_config=False` or remove the flag.'
    )

  config_path = xm.utils.resolve_path_relative_to_launcher(
      flags.FLAGS.xm_db_yaml_config_path
  )

  if not os.path.isfile(os.path.join(package_path, config_path)):
    shutil.copy(config_path, package_path)

  controller_args['xm_db_yaml_config_path'] = os.path.basename(config_path)


async def _launch_remote_controller(
    aux_unit: xm.ExperimentUnit,
    node: lp.PyNode,
    function_label: str,
    executor: xm.Executor,
    controller_name: str,
    controller_args: Dict[str, Any],
    controller_env_vars: Dict[str, str],
    package_path: str,
    use_host_db_config: bool,
) -> None:
  """Launches remote Job with the given controller."""
  package_path = xm.utils.resolve_path_relative_to_launcher(package_path)

  if use_host_db_config:
    _use_host_db_config(package_path, controller_args)

  docker_requirements = os.path.join(package_path, 'requirements.txt')
  docker_config = lp.DockerConfig(package_path, docker_requirements)

  executable_spec = _to_python_container(
      node, f'{aux_unit.experiment_unit_name}_{function_label}', docker_config
  )

  [executable] = await asyncio.get_running_loop().run_in_executor(
      None,
      aux_unit.experiment.package,
      [
          xm.Packageable(
              executable_spec=executable_spec,
              executor_spec=executor.Spec(),
          )
      ],
  )

  controller_job = xm.Job(
      name=controller_name,
      executable=executable,
      executor=executor,
      args=_parameter_controller_job_args(controller_args) or {},
      env_vars=controller_env_vars or {},
  )

  aux_unit.add(controller_job)


def _populate_flags(controller_args: Dict[str, Any]) -> None:
  """Sets flag values at runtime.

  This is meant to be used in conjunction with the `flags_to_populate`
  flags of `process_entry`. Since flag values can't be passed normally,
  this function sets values programmatically. This function is meant
  to be serialized and run inside `process_entry` through `_controller_body`.

  Args:
    controller_args: Mapping of flag names and values to be set.
  """
  for name, value in controller_args.items():
    flags.FLAGS[name].value = value


async def _controller_body(
    experiment_id, f, controller_args, *args, **kwargs
) -> None:
  _populate_flags(controller_args)

  async with xm_local.get_experiment(experiment_id) as experiment:
    await f(experiment, *args, **kwargs)


def controller(
    *,
    executor: xm.Executor,
    controller_name: str = 'parameter_controller',
    controller_env_vars: Optional[Dict[str, str]] = None,
    controller_args: Optional[Dict[str, Any]] = None,
    package_path: Optional[str] = '.',
    use_host_db_config: bool = True,
):
  """Converts a function to a controller which can be added to an experiment.

  Calling the wrapped function would return an xm.JobGenerator which would run
  it as auxiliary unit on the specified executor.

  Args:
    executor: The executor to launch the controller job on.
    controller_name: Name of the parameter controller job.
    controller_env_vars: Mapping of env variable names and values to be passed
      to the parameter controller job.
    controller_args: Mapping of flag names and values to be used by the XM
      client running inside the parameter controller job.
    package_path: Path of directory where the parameter controller container
      will be packaged. This directory must contain a `requirements.txt` file
      for the job, as well as other things necessary to run the controller job,
      like a DB YAML config or things for the to be launched jobs.
    use_host_db_config: Specifies if the DB config used by the host should be
      copied and used by the parameter controller job. Defaults to True. Can't
      be used in conjuction with passing the `xm_db_yaml_config` flag to the
      controller job.

  Returns:
    A decorator to be applied to the function.
  """

  def wrap(f: Callable[..., None]) -> Callable[..., xm.JobGeneratorType]:
    def make_controller(*args, **kwargs) -> xm.JobType:
      async def job_generator(aux_unit: xm.ExperimentUnit) -> None:
        experiment_id = aux_unit.experiment.experiment_id

        remote_controller = asyncio.create_task(
            _launch_remote_controller(
                aux_unit,
                lp.PyNode(
                    xm.run_in_asyncio_loop(_controller_body),
                    experiment_id,
                    f,
                    controller_args,
                    *args,
                    **kwargs,
                ),
                function_label=f.__name__,
                executor=executor,
                controller_name=controller_name,
                controller_args=controller_args,
                controller_env_vars=controller_env_vars,
                package_path=package_path,
                use_host_db_config=use_host_db_config,
            )
        )

        await remote_controller

      return xm.AuxiliaryUnitJob(
          job_generator,
          importance=xm.Importance.HIGH,
          # TODO: Add support for `termination_delay_secs`.
          termination_delay_secs=0,
      )

    return make_controller

  return wrap
