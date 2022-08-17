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
"""Module for getting the address of XManager jobs.

Addresses in XManager can be statically evaluated because the experiment ID is
known. Addressing should not involve tokens or late-bindings.
"""


def k8s_pod_domain(job_name: str,
                   experiment_id: int,
                   work_unit_id: int,
                   service: str = 'experiments',
                   namespace: str = 'default') -> str:
  """Returns the Kubernetes pod address of a job.

  Args:
    job_name: Job name.
    experiment_id: Experiment ID.
    work_unit_id: Work unit ID
    service: Name of the service for the job. Defaults to 'experiments'
    namespace: Namespace of the job. Defaults to 'default'
  """
  return (f'{experiment_id}-{work_unit_id}-{job_name}'
          f'.{service}.{namespace}.svc.cluster.local:2222')
