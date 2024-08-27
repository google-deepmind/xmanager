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
"""Local backend executables."""

from typing import Dict

import attr
from xmanager import xm


@attr.s(auto_attribs=True)
class LoadedContainerImage(xm.Executable):
  """A locally loaded container image."""

  image_id: str
  args: xm.SequentialArgs = attr.Factory(xm.SequentialArgs)
  env_vars: Dict[str, str] = attr.Factory(dict)


@attr.s(auto_attribs=True)
class LocalBinary(xm.Executable):
  """A locally located binary."""

  command: str
  args: xm.SequentialArgs = attr.Factory(xm.SequentialArgs)
  env_vars: Dict[str, str] = attr.Factory(dict)


@attr.s(auto_attribs=True)
class GoogleContainerRegistryImage(xm.Executable):
  """An image inside Google Container Registry."""

  image_path: str
  args: xm.SequentialArgs = attr.Factory(xm.SequentialArgs)
  env_vars: Dict[str, str] = attr.Factory(dict)
