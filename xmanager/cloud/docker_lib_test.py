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
"""Tests for xmanager.cloud.docker_lib."""

import os
import tarfile
import tempfile
import unittest

from xmanager.cloud import docker_lib


class DockerLibTest(unittest.TestCase):

  def test_build_tar(self):
    path = tempfile.mkdtemp()
    with open(os.path.join(path, 'train.py'), 'w') as f:
      f.write('print("Hello World")')
    project_name = 'project'
    _, entrypoint_file = tempfile.mkstemp()
    with open(entrypoint_file, 'w') as f:
      f.write('python train.py')
    _, dockerfile = tempfile.mkstemp()
    with open(dockerfile, 'w') as f:
      f.write('FROM hello-world\nENTRYPOINT ./entrypoint.sh')
    tar = docker_lib.build_tar(path, project_name, entrypoint_file, dockerfile)

    extracted = tempfile.mkdtemp()
    with tarfile.open(tar) as t:
      t.extractall(path=extracted)
    files = os.listdir(extracted)
    self.assertEqual(len(files), 3)

    train = os.path.join(extracted, 'project', 'train.py')
    with open(train) as f:
      self.assertEqual(f.read(), 'print("Hello World")')
    entrypoint = os.path.join(extracted, 'entrypoint.sh')
    with open(entrypoint) as f:
      self.assertEqual(f.read(), 'python train.py')
    dockerfile = os.path.join(extracted, 'Dockerfile')
    with open(dockerfile) as f:
      self.assertEqual(f.read(), 'FROM hello-world\nENTRYPOINT ./entrypoint.sh')


if __name__ == '__main__':
  unittest.main()
