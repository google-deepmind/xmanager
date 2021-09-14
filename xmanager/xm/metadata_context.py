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
"""An interface to manipulate and access experiment metadata.

Metadata is attached to a context and the context may belong to an experiment
or a work unit.
"""

from typing import Collection, List, Optional, Set
import attr


class ContextAnnotations:
  """Interface for managing annotations.

  Annotations are user-supplied attributes of a context, such as title or tags.
  Default method implementations are intentionally left blank so that backends
  only have to implement the subset they support.
  """

  @property
  def title(self) -> str:
    """An experiment title.

    To differentiate experiments from each other they can be given a human
    readable title. Same title can be reused for multiple experiments.
    """
    return ''

  def set_title(self, title: str) -> None:
    """Sets the context title."""


@attr.s(auto_attribs=True)
class MetadataContext:
  """Interface for managing metadata.

  The metadata context could be attached to an experiment or a work unit.


  Attributes:
    creator: The username of the creator of this context.
    annotations: User-modifiable annotations.
  """

  creator: str
  annotations: ContextAnnotations
