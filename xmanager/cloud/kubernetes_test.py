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
"""Tests for xmanager.cloud.kubernetes."""

import unittest

from xmanager import xm
from xmanager.cloud import kubernetes
from xmanager.xm_local import executors as local_executors


class KubernetesTest(unittest.TestCase):

  def test_requirements_from_executor(self):
    executor = local_executors.Kubernetes(
        requirements=xm.JobRequirements(cpu=1, ram=1 * xm.GiB))
    requirements = kubernetes.requirements_from_executor(executor).to_dict()
    self.assertDictEqual(requirements['limits'], {
        'cpu': '1',
        'memory': str(2**30),
    })

  def test_requirements_from_executor_gpu(self):
    executor = local_executors.Kubernetes(
        requirements=xm.JobRequirements(v100=4))
    requirements = kubernetes.requirements_from_executor(executor).to_dict()
    self.assertDictEqual(requirements['limits'], {'nvidia.com/gpu': '4'})

  def test_requirements_from_executor_empty(self):
    executor = local_executors.Kubernetes()
    requirements = kubernetes.requirements_from_executor(executor).to_dict()
    self.assertDictEqual(requirements['limits'], {})

  def test_annotations_from_executor_tpu(self):
    executor = local_executors.Kubernetes(xm.JobRequirements(tpu_v2=8))
    self.assertDictEqual(
        kubernetes.annotations_from_executor(executor),
        {'tf-version.cloud-tpus.google.com': 'nightly'})

  def test_annotations_from_executor_gpu(self):
    executor = local_executors.Kubernetes(xm.JobRequirements(v100=4))
    self.assertDictEqual(kubernetes.annotations_from_executor(executor), {})

  def test_node_selector_from_executor_gpu(self):
    executor = local_executors.Kubernetes(xm.JobRequirements(v100=4))
    self.assertDictEqual(
        kubernetes.node_selector_from_executor(executor),
        {'cloud.google.com/gke-accelerator': 'nvidia-tesla-v100'})

  def test_node_selector_from_executor_tpu(self):
    executor = local_executors.Kubernetes(xm.JobRequirements(tpu_v2=8))
    self.assertDictEqual(kubernetes.node_selector_from_executor(executor), {})

  def test_node_selector_from_executor_empty(self):
    executor = local_executors.Kubernetes(xm.JobRequirements())
    self.assertDictEqual(kubernetes.node_selector_from_executor(executor), {})


if __name__ == '__main__':
  unittest.main()
