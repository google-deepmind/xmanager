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
import atexit
import copy
import copyreg
import dataclasses
import functools
import os
import pathlib
import shutil
import sys
import tempfile
from typing import Any, Callable, Dict, Optional, Sequence

from absl import flags
import cloudpickle
from xmanager import xm
from xmanager import xm_local


_DATA_FILE_NAME = 'job.pkl'
_INIT_FILE_NAME = 'init.pkl'


class _PyNode:
  """User defined Python function."""

  def __init__(self, function: Callable[..., Any], *args, **kwargs):
    super().__init__()
    self._func_args = args
    self._func_kwargs = kwargs
    self._function = function
    self._partial_function = self._construct_partial_function

  @property
  def function(self) -> Callable[..., Any]:
    return self._partial_function

  def _construct_partial_function(self):
    return functools.partial(
        self._function, *self._func_args, **self._func_kwargs
    )()


@dataclasses.dataclass
class _DockerConfig:
  """Local docker launch configuration.

  Attributes:
    code_directory: Path to directory containing any user code that may be
      required inside the Docker image. The user code from this directory is
      copied over into the Docker containers, as the user code may be needed
      during program execution. If needed, modify docker_instructions in
      xm.PythonContainer construction below if user code needs installation.
    docker_requirements: Path to requirements.txt specifying Python packages to
      install inside the Docker image.
    hw_requirements: Hardware requirements.
    python_path: Additional paths to be added to PYTHONPATH prior to executing
      an entry point.
  """

  code_directory: str | None = None
  docker_requirements: str | None = None
  hw_requirements: xm.JobRequirements | None = None
  python_path: list[str] | None = None


@functools.lru_cache(maxsize=1)
def _enable_lru_cache_pickling_once():
  """Enables pickling for functools.lru_cache."""
  lru_cache_type = type(functools.lru_cache()(lambda: None))

  def new_lru_cache(func, cache_kwargs):
    return functools.lru_cache(**cache_kwargs)(func)

  def _pickle_lru_cache(obj):
    params = {}
    if hasattr(obj, 'cache_parameters'):
      params = obj.cache_parameters()
    return new_lru_cache, (obj.__wrapped__, params)

  copyreg.pickle(lru_cache_type, _pickle_lru_cache)


def _cloudpickle_dump_with_user_friendly_error(
    functions, description: str, file=None
):
  """Serializes functions, and throws user-friendly error upon failure."""
  try:
    if file:
      return cloudpickle.dump(functions, file)
    else:
      return cloudpickle.dumps(functions)
  except Exception as e:
    # When using `pdb`, we want to be able to go up the stack that goes into
    # the serialization error. Thus, we need to propagate the traceback.
    raise RuntimeError(
        str(e.__class__.__name__)
        + ': '
        + str(e)
        + '\n'
        f'The nodes associated to {description} were '
        'not serializable using cloudpickle.'
    ).with_traceback(e.__traceback__) from e


def _serialize_functions(data_file_path: str, description: str, functions):
  """Serializes into a file at path `data_file_path` for PyNode functions.

  Args:
    data_file_path: The path of the (local) file to write to.
    description: Describes the functions, e,g., the label of the group they
      belongs to. This is propagated to enrich the error message.
    functions: PyNode functions as a list or list-like object.
  """
  _enable_lru_cache_pickling_once()
  with open(data_file_path, 'wb') as f:
    _cloudpickle_dump_with_user_friendly_error(functions, description, f)


def _to_python_container(
    node: _PyNode, label: str, docker_config: _DockerConfig
) -> xm.PythonContainer:
  """Returns xm.PythonContainer embedding a PyNode."""
  return _to_docker_executables(
      [node], label=label, docker_config=docker_config
  )[0][0]


def _initializer(python_path):
  sys.path = python_path + sys.path


def _to_docker_executables(
    nodes: Sequence[Any],
    label: str,
    docker_config: _DockerConfig,
) -> list[tuple[xm.PythonContainer, xm.JobRequirements]]:
  """Returns a list of `PythonContainer`s objects for the given `PyNode`s."""
  if (
      docker_config.code_directory is None
      or docker_config.docker_requirements is None
  ):
    raise ValueError(
        'code_directory or docker_requirements must be specified through'
        'DockerConfig via local_resources when using "xm_docker" launch type.'
    )

  # Generate tmp dir without '_' in the name, Vertex AI fails otherwise.
  tmp_dir = '_'
  while '_' in tmp_dir:
    tmp_dir = tempfile.mkdtemp()
  atexit.register(shutil.rmtree, tmp_dir, ignore_errors=True)

  command_line = f'python -m process_entry --data_file={_DATA_FILE_NAME}'

  # Add common initialization function for all nodes which sets up PYTHONPATH.
  if docker_config.python_path:
    command_line += f' --init_file={_INIT_FILE_NAME}'
    # Local 'path' is copied under 'tmp_dir' (no /tmp prefix) inside Docker.
    python_path = [
        '/' + os.path.basename(tmp_dir) + os.path.abspath(path)
        for path in docker_config.python_path
    ]
    initializer_file_path = pathlib.Path(tmp_dir, _INIT_FILE_NAME)
    with open(initializer_file_path, 'wb') as f:
      cloudpickle.dump(functools.partial(_initializer, python_path), f)

  data_file_path = str(pathlib.Path(tmp_dir, _DATA_FILE_NAME))
  _serialize_functions(data_file_path, label, [n.function for n in nodes])

  file_path = pathlib.Path(__file__).absolute()

  shutil.copy(pathlib.Path(file_path.parent, 'process_entry.py'), tmp_dir)
  shutil.copytree(docker_config.code_directory, tmp_dir, dirs_exist_ok=True)
  shutil.copy(
      docker_config.docker_requirements,
      pathlib.Path(tmp_dir, 'requirements.txt'),
  )

  workdir_path = pathlib.Path(tmp_dir).name

  if not os.path.exists(docker_config.docker_requirements):
    raise FileNotFoundError(
        'Please specify a path to a file with Python package requirements '
        'through docker_config.docker_requirements.'
    )
  job_requirements = docker_config.hw_requirements
  if not job_requirements:
    job_requirements = xm.JobRequirements()

  # Make a copy of requirements since they are being mutated below.
  job_requirements = copy.deepcopy(job_requirements)

  if job_requirements.replicas != 1:
    raise ValueError(
        'Number of replicas is computed by the runtime. '
        'Please do not set it explicitly in the requirements.'
    )

  job_requirements.replicas = len(nodes)
  python_version = f'{sys.version_info.major}.{sys.version_info.minor}'
  base_image = f'python:{python_version}'
  return [(
      xm.PythonContainer(
          path=tmp_dir,
          base_image=base_image,
          entrypoint=xm.CommandList([command_line]),
          docker_instructions=[
              'RUN apt-get update && apt-get install -y git',
              'RUN python -m pip install --upgrade pip',
              f'COPY {workdir_path}/requirements.txt requirements.txt',
              'RUN python -m pip install xmanager',
              'RUN python -m pip install -r requirements.txt',
              f'COPY {workdir_path}/ {workdir_path}',
              f'WORKDIR {workdir_path}',
          ],
      ),
      job_requirements,
  )]


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
    node: _PyNode,
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
  docker_config = _DockerConfig(package_path, docker_requirements)

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
      args={
          **controller_args,
      },
      env_vars=controller_env_vars or {},
  )

  aux_unit.add(controller_job)


async def _controller_body(experiment_id, f, *args, **kwargs) -> None:
  # Note: Consider re-creating experiment DB in the remote controller.
  async with xm_local.create_experiment(
      f'experiment_id={experiment_id} controller={f.__name__}'
  ) as experiment:
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
                _PyNode(
                    xm.run_in_asyncio_loop(_controller_body),
                    experiment_id,
                    f,
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
