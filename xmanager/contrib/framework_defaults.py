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
"""Automatic selection of reasonable defaults based on input.

This library aimed to provide recommended defaults for the choices that can be
automated depending on the input. In particular, the choice of a base image
depends on a framework and accelerator used for the experiment, and can change
if a different accelerator is used. This module provides function
`base_image`, which returns the recommended base image to use without the need
to change the launcher's code.

Please note that the recommendations are subject to change as we are trying to
use the latest supported images for all frameworks. If a change in the
recommendation breaks your code, try to replace this call with the explicit
base image name.
"""

import enum
from typing import Optional, Union

from absl import logging
from xmanager import xm


class MLFramework(enum.Enum):
  """ML Framework used in the experiment code."""
  UNKNOWN = 0
  JAX = 1
  PYTORCH = 2
  TF2 = 3
  TF1 = 4  # Unsupported


FrameworkSpec = Union[str, MLFramework]


def _get_framework(framework: str) -> MLFramework:
  """Given a framework name in a loose form, returns the most suitable enum."""
  if (framework == 'jax' or framework == 'flax'):
    return MLFramework.JAX
  if 'torch' in framework:
    return MLFramework.PYTORCH
  if 'tf' in framework or 'tensorflow' in framework:
    # A variant of a Tensorflow.
    return MLFramework.TF1 if '1' in framework else MLFramework.TF2
  # Framework not recognized.
  logging.error('Unrecognized framework "%s"', framework)
  return MLFramework.UNKNOWN


def base_image(framework: FrameworkSpec,
               accelerator: Optional[xm.ResourceType]) -> str:
  """Returns a base image recommendation depending on the input.

  Please note that the recommendations can change as we are trying to recommend
  the latest supported images for all frameworks. Currently most of the images
  are taken from
  https://cloud.google.com/deep-learning-containers/docs/choosing-container.

  Args:
    framework: a free-text framework name. Recognized options are `jax`,
      `pytorch`, `tf`, `tensorflow`. The enum value is also accepted.
    accelerator: Accelerator specification from xm.JobRequirements. If None,
      then execution on CPU is assumed.

  Returns:
    a name of a base image to pass to e.g. xm.python_container.
  """
  if isinstance(framework, str):
    framework = _get_framework(framework)
  # LINT.IfChange(base_image_version)
  if framework == MLFramework.JAX:
    # JAX-based experiment use the same base image for all accelerators.
    return 'gcr.io/deeplearning-platform-release/base-cu113'
  elif framework == MLFramework.TF2:
    # TF experiments use the same base image for all accelerators.
    return 'gcr.io/deeplearning-platform-release/tf2-gpu.2-6'
  elif framework == MLFramework.PYTORCH:
    if accelerator in xm.TpuType:
      # Base image is taken from pytorch / XLA documentation.
      # https://github.com/pytorch/xla#-available-images-and-wheels
      return 'gcr.io/tpu-pytorch/xla:nightly_3.8_tpuvm_20220819'
    else:
      return 'gcr.io/deeplearning-platform-release/pytorch-gpu.1-12'
  # LINT.ThenChange(:tpu_runtime_version)
  elif framework == MLFramework.TF1:
    logging.warning('Tensorflow 1.x is not supported')
    return 'gcr.io/deeplearning-platform-release/tf-gpu.1-15'
  else:
    # Unrecognized framework: use the default CUDA image.
    return 'gcr.io/deeplearning-platform-release/base-cu113'
