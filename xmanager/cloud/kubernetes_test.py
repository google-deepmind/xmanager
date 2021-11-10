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
import json
import unittest
from unittest import mock

from kubernetes import client as k8s_client

from xmanager import xm
from xmanager.cloud import kubernetes
from xmanager.xm_local import executables as local_executables
from xmanager.xm_local import executors as local_executors


class CallAPIResponse:
  items = []


class KubernetesTest(unittest.TestCase):

  def test_launch(self):
    fake_client = mock.Mock()
    fake_client.call_api.return_value = CallAPIResponse()
    client = kubernetes.Client(fake_client)

    job = xm.Job(
        name='test-job',
        executable=local_executables.GoogleContainerRegistryImage(
            name='test-image',
            image_path='image-path',
            args=xm.SequentialArgs.from_collection({'a': 1}),
        ),
        executor=local_executors.Kubernetes(
            xm.JobRequirements(cpu=1, ram=1, t4=2)),
        args={
            'b': 2,
            'c': 3
        },
    )
    expected_service = k8s_client.V1Service(
        metadata=k8s_client.V1ObjectMeta(name='experiments'),
        spec=k8s_client.V1ServiceSpec(
            selector={'service': 'experiments'},
            cluster_ip='None',
        ),
    )
    cluster_spec = json.dumps({
        'cluster': {
            'workerpool0': [
                'workerpool0.experiments.default.svc.cluster.local:2222'
            ]
        },
        'task': {
            'type': 'workerpool0',
            'index': 0
        }
    })
    expected_job = k8s_client.V1Job(
        metadata=k8s_client.V1ObjectMeta(name='test-job'),
        spec=k8s_client.V1JobSpec(
            template=k8s_client.V1PodTemplateSpec(
                metadata=k8s_client.V1ObjectMeta(
                    labels={'service': 'experiments'},
                    annotations={},
                ),
                spec=k8s_client.V1PodSpec(
                    hostname='workerpool0',
                    subdomain='experiments',
                    restart_policy='Never',
                    containers=[
                        k8s_client.V1Container(
                            name='test-job',
                            image='image-path',
                            resources=k8s_client.V1ResourceRequirements(
                                limits={
                                    'cpu': '1',
                                    'memory': '1',
                                    'nvidia.com/gpu': '2',
                                },),
                            args=['--a=1', '--b=2', '--c=3'],
                            env=[
                                k8s_client.V1EnvVar(
                                    'CLUSTER_SPEC',
                                    cluster_spec,
                                )
                            ],
                        )
                    ],
                    node_selector={
                        'cloud.google.com/gke-accelerator': 'nvidia-tesla-t4',
                    },
                ),
            ),
            backoff_limit=0,
        ),
    )

    client.launch(lambda x: x, [job])
    [_, service_call, job_call] = fake_client.call_api.call_args_list
    _, service_kwargs = service_call
    self.assertEqual(service_kwargs['body'], expected_service)
    _, job_kwargs = job_call
    self.assertEqual(job_kwargs['body'], expected_job)

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
