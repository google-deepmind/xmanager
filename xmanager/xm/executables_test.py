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
"""Tests for executables."""

import os
import unittest

from xmanager.xm import executables
from xmanager.xm import utils


class ExecutablesTest(unittest.TestCase):

  def test_python_container_name(self):
    executable = executables.PythonContainer(
        entrypoint=executables.ModuleName('module'),
        path='/home/user/project/',
    )

    self.assertEqual(executable.name, 'project')

  def test_container_name(self):
    executable = executables.Container(
        image_path='/home/user/project/image.tar')

    self.assertEqual(executable.name, 'image_tar')

  def test_binary_name(self):
    executable = executables.Binary(path='./binary')

    self.assertEqual(executable.name, 'binary')

  def test_bazel_container_name(self):
    executable = executables.BazelContainer(label='//container')

    self.assertEqual(executable.name, 'container')

  def test_bazel_binary_name(self):
    executable = executables.BazelBinary(label=':binary')

    self.assertEqual(executable.name, '_binary')

  def test_dockerfile_defaults(self):
    root = utils.resolve_path_relative_to_launcher('.')

    spec = executables.Dockerfile()
    self.assertEqual(spec.path, root)
    self.assertEqual(spec.dockerfile, os.path.join(root, 'Dockerfile'))


if __name__ == '__main__':
  unittest.main()
