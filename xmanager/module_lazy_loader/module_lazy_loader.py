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
"""Custom lazy loader definition for xmanager sub-package __init__.py files."""

import dataclasses
import importlib
import sys
import types
from typing import Any, Callable, Optional, Sequence


@dataclasses.dataclass
class XManagerAPI:
  """Dataclass for XManager sub-package APIs.

  Attributes:
    module: An XManager submodule, which will be the exposed API if no symbol is
      provided.
    symbol: An optional symbol to expose from the given module. If provided, the
      API will be module.symbol.
    alias: An optional alias to rename the API.
  """

  module: str
  symbol: Optional[str] = None
  alias: Optional[str] = None


class XManagerLazyLoader:
  """Custom lazy loader for xmanager sub-package __init__.py files."""

  def __init__(self, subpackage_name: str, apis: Sequence[XManagerAPI]):
    """Initializes the XManagerLazyLoader.

    Args:
      subpackage_name: The name of the current xmanager sub-package (i.e. the
        __name__ attribute of the current xmanager sub-package).
      apis: A list of XManagerAPIs to expose from the XManager sub-package.
    """
    self.subpackage_name = subpackage_name
    self.apis = apis
    self._loaded_attrs = {}
    self._name_to_api: dict[str, XManagerAPI] = {}

    for api in self.apis:
      if api.alias:
        name = api.alias
      elif api.symbol:
        name = api.symbol
      else:
        name = api.module.split(".")[-1]  # module name
      self._name_to_api[name] = api

  def get_module_all(self) -> list[str]:
    """Returns __all__ for the xmanager sub-package __init__.py file."""
    return list(self._name_to_api.keys())

  def get_module_dir(self) -> Callable[[], list[str]]:
    """Returns __dir__ for the xmanager sub-package __init__.py file."""
    return lambda: sorted(self._name_to_api.keys())

  def get_module_getattr(
      self,
  ) -> Callable[[str], types.ModuleType | Any | None]:
    """Returns __getattr__ for the xmanager sub-package __init__.py file."""

    def _import_module_with_reloaded_parent(module_name: str, e: ImportError):
      # reload module's parent as a last resort (likely in the case that a
      # module was imported outside adhoc import context but later
      # used within it). Assuming the parent package has a lazy-loaded
      # / empty __init__.py file, this should be quick.
      if e.name:
        parent = e.name.rsplit(".", 1)[0]
        parent_module = importlib.import_module(parent)
        importlib.reload(parent_module)
        return importlib.import_module(module_name)
      else:
        raise e

    def _import_module(module_name: str):
      try:
        return importlib.import_module(module_name)
      except ImportError as e:
        return _import_module_with_reloaded_parent(e)

    def _module_getattr(name: str) -> types.ModuleType | Any | None:
      if name in self._loaded_attrs:
        return self._loaded_attrs[name]
      if name in self._name_to_api:
        api = self._name_to_api[name]
        module = _import_module(api.module)
        if api.symbol:
          attr = getattr(module, api.symbol)
          self._loaded_attrs[name] = attr
          return attr
        else:
          self._loaded_attrs[name] = module
          return module
      raise AttributeError(
          f"module {self.subpackage_name!r} has no attribute {name!r}"
      )

    return _module_getattr
