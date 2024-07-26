"""Tests for xmanager.xm_local.executors."""

import unittest
from unittest import mock

from xmanager.xm_local import executors


class ExecutorsTest(unittest.TestCase):

  @mock.patch.object(executors, 'importlib')
  def test_executor_missing_required_module(self, mock_importlib):
    mock_importlib.import_module.side_effect = ModuleNotFoundError(
        "No module named 'xmanager.cloud.kubernetes'"
    )
    with self.assertRaises(ModuleNotFoundError):
      executors.Kubernetes()


if __name__ == '__main__':
  unittest.main()
