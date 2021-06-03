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
"""Tests for factories."""

import unittest

from xmanager.xm import core
from xmanager.xm import executables
from xmanager.xm import factories
from xmanager.xm_local import executors


class FactoriesTest(unittest.TestCase):

  def test_minimal_executable_spec(self):
    expected = core.Packageable(
        executable_spec=executables.BazelBinary(label='label'),
        executor_spec=executors.Local.Spec(),
        args=[],
        env_vars={},
    )

    actual = factories.bazel_binary(executors.Local.Spec(), label='label')

    self.assertEqual(actual, expected)

  def test_pkg_args_env_vars(self):
    expected = core.Packageable(
        executable_spec=executables.BazelBinary(label='label'),
        executor_spec=executors.Local.Spec(),
        args=['-f'],
        env_vars={'KEY': 'value'},
    )

    actual = factories.bazel_binary(
        executors.Local.Spec(),
        label='label',
        args=['-f'],
        env_vars={'KEY': 'value'},
    )

    self.assertEqual(actual, expected)


if __name__ == '__main__':
  unittest.main()
