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

import unittest

from xmanager.xm_local.packaging import bazel_tools


class BazelToolsTest(unittest.TestCase):

  def test_lex_full_label(self):
    self.assertEqual(
        bazel_tools._lex_label('//project/directory:target'),
        (['project', 'directory'], 'target'))

  def test_lex_short_label(self):
    self.assertEqual(
        bazel_tools._lex_label('//project/package'),
        (['project', 'package'], 'package'))

  def test_lex_root_target(self):
    self.assertEqual(bazel_tools._lex_label('//:label'), ([], 'label'))

  def test_lex_empty_label(self):
    with self.assertRaises(ValueError):
      bazel_tools._lex_label('//')

  def test_lex_relative_label(self):
    with self.assertRaises(ValueError):
      bazel_tools._lex_label('a/b:c')

  def test_assemble_label(self):
    self.assertEqual(bazel_tools._assemble_label((['a', 'b'], 'c')), '//a/b:c')

  def test_label_kind_lines_to_dict(self):
    self.assertEqual(
        bazel_tools._label_kind_lines_to_dict([
            'py_binary rule //:py_target',
            'cc_binary rule //:cc_target',
        ]),
        {
            '//:py_target': 'py_binary rule',
            '//:cc_target': 'cc_binary rule'
        },
    )


if __name__ == '__main__':
  unittest.main()
