import unittest

from xmanager.bazel import client


class BazelTargetTest(unittest.TestCase):

  def test_equality_same_label_and_args(self):
    target1 = client.BazelTarget(
        label="//foo:bar", bazel_args=["--flag1", "--flag2"]
    )
    target2 = client.BazelTarget(
        label="//foo:bar", bazel_args=["--flag1", "--flag2"]
    )
    self.assertEqual(target1, target2)

  def test_equality_same_label_different_args_order(self):
    target1 = client.BazelTarget(
        label="//foo:bar", bazel_args=["--flag1", "--flag2"]
    )
    target2 = client.BazelTarget(
        label="//foo:bar", bazel_args=["--flag2", "--flag1"]
    )
    self.assertEqual(target1, target2)

  def test_equality_same_label_different_args(self):
    target1 = client.BazelTarget(label="//foo:bar", bazel_args=["--flag1"])
    target2 = client.BazelTarget(label="//foo:bar", bazel_args=["--flag2"])
    self.assertNotEqual(target1, target2)


if __name__ == "__main__":
  unittest.main()
