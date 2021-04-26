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
"""Tests for xmanager.cloud.utils."""

import os
import tempfile
import unittest

from xmanager.cloud import utils

_CLUSTER_SPEC = """{
  "cluster": {
    "workerpool0": ["cmle-training-workerpool0-ab-0:2222"],
    "workerpool1": ["cmle-training-workerpool1-ab-0:2222", "cmle-training-workerpool1-ab-1:2222"],
    "workerpool2": ["cmle-training-workerpool2-ab-0:2222", "cmle-training-workerpool2-ab-1:2222"]
  },
  "environment": "cloud",
  "task": {
    "type": "workerpool1",
    "index": 1,
    "trial": ""
  }
}""".replace('\n', ' ')


class UtilsTest(unittest.TestCase):

  def tearDown(self):
    super(UtilsTest, self).tearDown()
    os.environ['CLUSTER_SPEC'] = ''

  def test_get_master_address_port(self):
    os.environ['CLUSTER_SPEC'] = _CLUSTER_SPEC
    address, port = utils.get_master_address_port()
    self.assertEqual(address, 'cmle-training-workerpool0-ab-0')
    self.assertEqual(port, '2222')

  def test_get_master_address_port_default(self):
    address, port = utils.get_master_address_port()
    self.assertEqual(address, '127.0.0.1')
    self.assertEqual(port, '29500')

  def test_get_world_size_rank(self):
    os.environ['CLUSTER_SPEC'] = _CLUSTER_SPEC
    world_size, rank = utils.get_world_size_rank()
    self.assertEqual(world_size, 5)
    self.assertEqual(rank, 2)

  def test_get_world_size_rank_default(self):
    world_size, rank = utils.get_world_size_rank()
    self.assertEqual(world_size, 1)
    self.assertEqual(rank, 0)

  def test_wrap_and_unwrap_addresses(self):
    arg = '--master=' + utils.get_workerpool_address('workerpool0')
    self.assertEqual(arg, '--master=%objectname(workerpool0)%')
    os.environ['CLUSTER_SPEC'] = _CLUSTER_SPEC
    self.assertEqual(
        utils.map_workerpool_address_args([arg]),
        ['--master=cmle-training-workerpool0-ab-0:2222'])

  def test_create_workerpool_address_env_vars_script(self):
    os.environ['MY_WORKER'] = utils.get_workerpool_address('workerpool0')
    os.environ['CLUSTER_SPEC'] = _CLUSTER_SPEC
    t = tempfile.NamedTemporaryFile()
    utils.create_workerpool_address_env_vars_script(t.name)
    expected = """
#!/bin/bash

export MY_WORKER=cmle-training-workerpool0-ab-0:2222
    """
    with open(t.name) as f:
      self.assertEqual(f.read(), expected.strip())


if __name__ == '__main__':
  unittest.main()
