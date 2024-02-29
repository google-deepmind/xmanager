# Copyright 2024 DeepMind Technologies Limited
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
"""Test for custom lazy loader definition for xmanager sub-package __init__.py files."""

import unittest
from xmanager import module_lazy_loader


class ModuleLazyLoaderTest(unittest.TestCase):
  test_lazy_loader = module_lazy_loader.XManagerLazyLoader(
      __name__,
      apis=[
          module_lazy_loader.XManagerAPI(
              module="xmanager.module_lazy_loader",
              symbol="XManagerAPI",
              alias="boo",
          ),
          module_lazy_loader.XManagerAPI(
              module="xmanager.module_lazy_loader",
              symbol="XManagerAPI",
          ),
          module_lazy_loader.XManagerAPI(
              module="xmanager.module_lazy_loader", alias="baz"
          ),
          module_lazy_loader.XManagerAPI(
              module="xmanager.module_lazy_loader",
          ),
      ],
  )

  def test_all(self):
    self.assertCountEqual(
        self.test_lazy_loader.get_module_all(),
        ["boo", "XManagerAPI", "baz", "module_lazy_loader"],
    )

  def test_dir(self):
    self.assertCountEqual(
        self.test_lazy_loader.get_module_dir()(),
        ["boo", "XManagerAPI", "baz", "module_lazy_loader"],
    )

  def test_getattr(self):
    local_getattr = self.test_lazy_loader.get_module_getattr()
    self.assertEqual(local_getattr("boo"), module_lazy_loader.XManagerAPI)
    self.assertEqual(
        local_getattr("XManagerAPI"), module_lazy_loader.XManagerAPI
    )
    self.assertEqual(local_getattr("baz"), module_lazy_loader)
    self.assertEqual(local_getattr("module_lazy_loader"), module_lazy_loader)
    self.assertRaises(AttributeError, local_getattr, "this_attr_does_not_exist")


if __name__ == "__main__":
  unittest.main()
