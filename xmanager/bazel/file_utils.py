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
"""A collection of tools for files operations."""

import os
import tempfile


class TemporaryFilePath:
  """A context manager providing a temporary file path.

  Unlike NamedTemporaryFile, TemporaryFilePath closes the file when one enters
  the context.
  """

  _path: str

  def __enter__(self):
    fd, path = tempfile.mkstemp()
    os.close(fd)
    self._path = path
    return path

  def __exit__(self, error_type, error_value, traceback):
    os.remove(self._path)
