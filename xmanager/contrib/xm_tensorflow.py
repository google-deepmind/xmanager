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
"""Tools for setting up distributed TF experiments.

Supported distributed setups:

- tf.distribute.MultiWorkerMirroredStrategy
- tf.distribute.ParameterServerStrategy
"""

import json
from typing import Awaitable, Callable

import attr
from xmanager import xm
from xmanager import xm_local
from xmanager.contrib import addressing


@attr.s(auto_attribs=True)
class MultiWorkerMirroredStrategyBuilder:
  """Run a Tensorflow MultiWorkerMirroredStrategy experiment.

  https://www.tensorflow.org/api_docs/python/tf/distribute/MultiWorkerMirroredStrategy

  Usage:
    builder = MultiWorkerMirroredStrategyBuilder(
        experiment, worker_executable, worker_executor, num_workers=4)
    for hparams in hyperparameters:
      experiment.add(build.gen_job_group(), hparams)
  """

  experiment: xm.Experiment
  worker_executable: xm.Executable
  worker_executor: xm.Executor
  worker_name: str = 'worker'
  num_workers: int = 1

  def create_job_group(
      self, work_unit: xm.WorkUnit, hparams: xm.UserArgs
  ) -> xm.JobGroup:
    if isinstance(self.worker_executor, xm_local.Kubernetes):
      return self.create_kubernetes_job_group(work_unit, hparams)

    raise NotImplementedError(
        'MultiWorkerMirrored is not supported for executor_type '
        f'`{type(self.worker_executor)}`'
    )

  def gen_job_group(
      self,
  ) -> Callable[[xm.WorkUnit], Awaitable[Awaitable[None]]]:
    """Create a generator that can be be used with experiment.add(generator)."""

    async def _gen_job_group(
        work_unit: xm.WorkUnit, **hparams
    ) -> Awaitable[None]:
      job = self.create_job_group(work_unit, hparams)
      return work_unit.add(job)

    return _gen_job_group

  def create_kubernetes_job_group(
      self, work_unit: xm.WorkUnit, hparams: xm.UserArgs
  ) -> xm.JobGroup:
    """Builds a Kubernetes job group that can be added to an experiment."""
    assert isinstance(self.worker_executor, xm_local.Kubernetes)

    worker_job_domains = {}
    for i in range(self.num_workers):
      job_name = f'{self.worker_name}-{i}'

      worker_job_domains[job_name] = addressing.k8s_pod_domain(
          job_name=job_name,
          experiment_id=self.experiment.experiment_id,
          work_unit_id=work_unit.work_unit_id,
      )

    jobs = {}
    for i, worker_job_name in enumerate(worker_job_domains):
      tf_config = {
          'cluster': {'worker': list(worker_job_domains.values())},
          'task': {'type': 'worker', 'index': i},
      }

      jobs[worker_job_name] = xm.Job(
          executable=self.worker_executable,
          executor=self.worker_executor,
          args=hparams,
          env_vars={
              'TF_CONFIG': json.dumps(tf_config),
          },
      )

    return xm.JobGroup(**jobs)


@attr.s(auto_attribs=True)
class ParameterServerStrategyBuilder:
  """Builds a Tensorflow ParameterServer experiment in XManager.

  https://www.tensorflow.org/api_docs/python/tf/distribute/experimental/ParameterServerStrategy

  Usage:
    builder = ParameterServerStrategyBuilder(experiment)
    for hparams in hyperparameters:
      experiment.add(builder.gen_job_group(), hparams)
  """

  experiment: xm.Experiment
  chief_executable: xm.Executable
  chief_executor: xm.Executor
  worker_executable: xm.Executable
  worker_executor: xm.Executor
  ps_executable: xm.Executable
  ps_executor: xm.Executor
  chief_name: str = 'chief'
  worker_name: str = 'worker'
  ps_name: str = 'ps'
  num_workers: int = 1
  num_ps: int = 1

  def create_job_group(
      self, work_unit: xm.WorkUnit, hparams: xm.UserArgs
  ) -> xm.JobGroup:
    if isinstance(self.worker_executor, xm_local.Kubernetes):
      return self.create_kubernetes_job_group(work_unit, hparams)

    raise NotImplementedError(
        'ParameterServerStrategy is not supported for executor_type '
        f'`{type(self.worker_executor)}`'
    )

  def gen_job_group(
      self,
  ) -> Callable[[xm.WorkUnit], Awaitable[Awaitable[None]]]:
    """Create a generator that can be be used with experiment.add(generator)."""

    async def _gen_job_group(
        work_unit: xm.WorkUnit, **hparams
    ) -> Awaitable[None]:
      job = self.create_job_group(work_unit, hparams)
      return work_unit.add(job)

    return _gen_job_group

  def create_kubernetes_job_group(
      self, work_unit: xm.WorkUnit, hparams: xm.UserArgs
  ) -> xm.JobGroup:
    """Builds a Kubernetes job group that can be added to an experiment."""
    assert isinstance(self.chief_executor, xm_local.Kubernetes)
    assert isinstance(self.worker_executor, xm_local.Kubernetes)
    assert isinstance(self.ps_executor, xm_local.Kubernetes)

    def _k8s_pod_domain(job_name: str) -> str:
      return addressing.k8s_pod_domain(
          job_name, self.experiment.experiment_id, work_unit.work_unit_id
      )

    chief_domain = _k8s_pod_domain(self.chief_name)
    # pylint: disable=g-complex-comprehension
    worker_domains = [
        addressing.k8s_pod_domain(
            f'{self.worker_name}-{i}',
            self.experiment.experiment_id,
            work_unit.work_unit_id,
        )
        for i in range(self.num_workers)
    ]
    ps_domains = [
        addressing.k8s_pod_domain(
            f'{self.ps_name}-{i}',
            self.experiment.experiment_id,
            work_unit.work_unit_id,
        )
        for i in range(self.num_ps)
    ]

    # pylint: enable=g-complex-comprehension

    def _create_tf_config(task_type, task_index):
      return {
          'cluster': {
              'chief': [chief_domain],
              'worker': worker_domains,
              'ps': ps_domains,
          },
          'task': {
              'type': task_type,
              'index': task_index,
          },
      }

    jobs = {}
    jobs[self.chief_name] = xm.Job(
        executable=self.chief_executable,
        executor=self.chief_executor,
        env_vars={'TF_CONFIG': json.dumps(_create_tf_config('chief', 0))},
    )

    for i in range(self.num_ps):
      ps_job_name = f'{self.ps_name}-{i}'

      jobs[ps_job_name] = xm.Job(
          executable=self.ps_executable,
          executor=self.ps_executor,
          args=hparams,
          env_vars={
              'TF_CONFIG': json.dumps(_create_tf_config('ps', i)),
          },
      )

    for i in range(self.num_workers):
      worker_job_name = f'{self.worker_name}-{i}'

      jobs[worker_job_name] = xm.Job(
          executable=self.worker_executable,
          executor=self.worker_executor,
          args=hparams,
          env_vars={
              'TF_CONFIG': json.dumps(_create_tf_config('worker', i)),
          },
      )

    return xm.JobGroup(**jobs)
