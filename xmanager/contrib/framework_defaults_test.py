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

from absl.testing import absltest
from absl.testing import parameterized
from xmanager import xm
from xmanager.contrib import framework_defaults

MLFramework = framework_defaults.MLFramework


class FrameworkDefaultsTest(parameterized.TestCase):

  def test_known_frameworks(self):
    self.assertEqual(
        framework_defaults._get_framework('torch'), MLFramework.PYTORCH)
    self.assertEqual(
        framework_defaults._get_framework('pytorch'), MLFramework.PYTORCH)
    self.assertEqual(framework_defaults._get_framework('tf'), MLFramework.TF2)
    self.assertEqual(framework_defaults._get_framework('tf1'), MLFramework.TF1)
    self.assertEqual(framework_defaults._get_framework('tf2'), MLFramework.TF2)
    self.assertEqual(
        framework_defaults._get_framework('tensorflow 2.x'), MLFramework.TF2)
    self.assertEqual(framework_defaults._get_framework('jax'), MLFramework.JAX)
    self.assertEqual(framework_defaults._get_framework('flax'), MLFramework.JAX)

  def test_unknown_frameworks(self):
    self.assertEqual(
        framework_defaults._get_framework('huggingface'), MLFramework.UNKNOWN)
    self.assertEqual(
        framework_defaults._get_framework('objax'), MLFramework.UNKNOWN)
    self.assertEqual(
        framework_defaults._get_framework('not a framework name'),
        MLFramework.UNKNOWN)

  @parameterized.named_parameters(
      ('cpu', None),
      ('gpu', xm.ResourceType.V100),
      ('tpu', xm.ResourceType.TPU_V3),
  )
  def test_jax_base_image(self, accelerator):
    base_image = framework_defaults.base_image(MLFramework.JAX, accelerator)
    self.assertStartsWith(base_image, 'gcr.io/deeplearning-platform-release/')
    # Jax uses CUDA images.
    self.assertContainsSubsequence(base_image, 'cu')

  @parameterized.named_parameters(
      ('cpu', None),
      ('gpu', xm.ResourceType.V100),
      ('tpu', xm.ResourceType.TPU_V3),
  )
  def test_tf2_base_image(self, accelerator):
    base_image = framework_defaults.base_image(MLFramework.TF2, accelerator)
    self.assertStartsWith(base_image, 'gcr.io/deeplearning-platform-release/')
    self.assertContainsSubsequence(base_image, 'tf2-')

  @parameterized.named_parameters(
      ('cpu', None),
      ('gpu', xm.ResourceType.V100),
      ('tpu', xm.ResourceType.TPU_V3),
  )
  def test_torch_base_image(self, accelerator):
    base_image = framework_defaults.base_image(MLFramework.PYTORCH, accelerator)
    self.assertStartsWith(base_image, 'gcr.io/')
    self.assertContainsSubsequence(base_image, 'pytorch')
    if accelerator in xm.TpuType:
      self.assertContainsSubsequence(base_image, 'xla')

  @parameterized.named_parameters(
      ('cpu', None),
      ('gpu', xm.ResourceType.V100),
      ('tpu', xm.ResourceType.TPU_V3),
  )
  def test_unsupported_tf1_base_image(self, accelerator):
    base_image = framework_defaults.base_image(MLFramework.TF1, accelerator)
    self.assertStartsWith(base_image, 'gcr.io/deeplearning-platform-release/')
    self.assertContainsSubsequence(base_image, 'tf')

  @parameterized.named_parameters(
      ('cpu', None),
      ('gpu', xm.ResourceType.V100),
      ('tpu', xm.ResourceType.TPU_V3),
  )
  def test_unknown_base_image(self, accelerator):
    base_image = framework_defaults.base_image(MLFramework.UNKNOWN, accelerator)
    self.assertStartsWith(base_image, 'gcr.io/deeplearning-platform-release/')
    self.assertContainsSubsequence(base_image, 'base')


if __name__ == '__main__':
  absltest.main()
