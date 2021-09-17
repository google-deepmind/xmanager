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
"""A utility to predict IDs that would be assigned upon object creation.

This class helps to untie this chicken and egg problem. Sometimes to create an
object (for example WorkUnit) one may need to know its ID beforehand (for
example to generate a checkpoint path). But the ID would only be assigned by a
backend upon object creation. In ideal world we would rewrite the backend to
allow ID reservation. This module provides a temporary solution which does the
reservation on client-side. Following assumptions are made:
  * Ids are assigned sequentially by the backend, starting from some number.
  * Only one process at a time creates the objects. Any races are resolved only
    within that process.

Usage:
  predictor = Predictor()

  # Obtain the ID and asynchronously construct the object.
  next_id = predictor.reserve_id()
  job = Job(args={'checkpoint_path': f'/tmp/{next_id}'})

  # Wait untill all objects with smaller IDs are submitted.
  async with predictor.submit_id(next_id):
    # And submit it to the backend.
    submit(job)

If the submission fails, the sequence is considered broken and following calls
to the predictor would raise an error.
"""

import asyncio
import threading
from typing import AsyncIterable

import async_generator


class BrokenSequenceError(RuntimeError):
  """The ID would never be ready to submit."""


class Predictor:
  """Predicts IDs that would be assigned on object creation.

  This class is thread safe and async Python friendly.
  """

  def __init__(self, next_id: int) -> None:
    """Initializes the predictor.

    Args:
      next_id: The first available ID that would be assigned to the next object.
    """
    self._next_id = next_id
    # We use threading.Lock to allow calling reserve_id from non async context.
    # Note that no long operations are done under this lock.
    self._next_id_lock = threading.Lock()

    self._is_broken = False
    self._last_created_id = next_id - 1
    self._last_created_id_condition = asyncio.Condition()

  def reserve_id(self) -> int:
    """Returns the next ID."""
    with self._next_id_lock:
      next_id = self._next_id
      self._next_id += 1
      return next_id

  @async_generator.asynccontextmanager
  async def submit_id(self, id_to_submit: int) -> AsyncIterable[None]:
    """Waits until the ID can be submitted and marks it as such.

    A context manager which would wait for all smaller IDs to be submitted on
    entry and marks it as submitted on exit. As a result all submissions are
    serialized in the correct order and receive the right ID from the backend.

    Args:
      id_to_submit: The id to be submitted.

    Yields:
      Yields when it is time to send the request to the backend.
    """
    async with self._last_created_id_condition:
      await self._last_created_id_condition.wait_for(
          lambda: self._is_broken or self._last_created_id == id_to_submit - 1)

      try:
        if self._is_broken:
          raise BrokenSequenceError(
              f'Id {id} would never be ready to submit as'
              ' submission of the previous one has failed')
        yield
        self._last_created_id = id_to_submit
      except:
        self._is_broken = True
        raise
      finally:
        self._last_created_id_condition.notify_all()
