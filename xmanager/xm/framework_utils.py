# Copyright 2026 DeepMind Technologies Limited
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
"""Framework usage labeling for XManager launch scripts."""

from collections.abc import Callable
import contextvars
import functools
import inspect
import re
from typing import Any, TypeVar, cast

_ACTIVE_FRAMEWORK_LABELS: contextvars.ContextVar[tuple[str, ...]] = (
    contextvars.ContextVar('_ACTIVE_FRAMEWORK_LABELS', default=())
)

# Labels are propagated to the execution backend as `<key>:<value>` entries
# within a quoted config string, so a value must not contain the `:` delimiter,
# quotes, or whitespace. They are also grouping keys in usage reports, where
# stray punctuation would silently split one framework into several.
_VALID_LABEL_PATTERN = re.compile(r'[a-zA-Z0-9_-]+')

_CallableT = TypeVar('_CallableT', bound=Callable[..., Any])


def framework_label(label: str) -> '_FrameworkLabel':
  """Declares framework identity for XManager launch scripts.

  Frameworks that launch experiments on a user's behalf use this to identify
  themselves, so that the execution backend can record which framework created
  a job.

  Usage as a function decorator (`def` or `async def`):

    @xm.framework_label('my_framework')
    def launch(experiment, config):
      experiment.add(...)

  Usage as a context manager (within functions):

    with xm.framework_label('my_framework'):
      experiment.add(...)

  Args:
    label: A framework identifier to associate with any work units added within
      this scope, such as 'my_framework'. Must be a non-empty string of ASCII
      letters, digits, underscores and hyphens. Re-declaring a label that is
      already active is a no-op.

  Returns:
    A context manager that is also usable as a function decorator.

  Raises:
    ValueError: If `label` is empty or contains unsupported characters.
  """
  return _FrameworkLabel(label)


def get_active_framework_labels() -> list[str]:
  """Returns the currently active framework labels, outermost first."""
  return list(_ACTIVE_FRAMEWORK_LABELS.get())


class _FrameworkLabel:
  """The context manager / decorator returned by `framework_label()`."""

  def __init__(self, label: str):
    # `fullmatch` rather than `match`, which would accept a trailing newline.
    if not _VALID_LABEL_PATTERN.fullmatch(label):
      raise ValueError(
          'Framework label must be a non-empty string of ASCII letters,'
          f' digits, underscores and hyphens, got {label!r}.'
      )
    self._label = label
    # One token per currently open `with` block, so that the same object may be
    # entered more than once.
    self._tokens: list[contextvars.Token[tuple[str, ...]] | None] = []

  def __enter__(self) -> None:
    active = _ACTIVE_FRAMEWORK_LABELS.get()
    if self._label in active:
      # Re-entering an already active label is a no-op: a framework calling
      # into itself must neither duplicate its label nor fail. Nothing to undo.
      self._tokens.append(None)
    else:
      self._tokens.append(_ACTIVE_FRAMEWORK_LABELS.set(active + (self._label,)))

  def __exit__(self, *exc_info: Any) -> None:
    token = self._tokens.pop()
    if token is not None:
      _ACTIVE_FRAMEWORK_LABELS.reset(token)

  def __call__(self, func: _CallableT) -> _CallableT:
    """Returns `func` wrapped so the label is active during each call."""
    if inspect.isgeneratorfunction(func) or inspect.isasyncgenfunction(func):
      raise TypeError(
          'framework_label cannot decorate the generator function'
          f' {getattr(func, "__qualname__", func)!r}: calling it only builds a'
          ' generator, so the label would be dropped again before the body'
          ' runs. Use framework_label as a context manager inside the body'
          ' instead.'
      )

    if inspect.iscoroutinefunction(func):
      # This must be an `async def` rather than a plain function returning a
      # coroutine: `xm.Experiment.add()` checks job generators with
      # `inspect.iscoroutinefunction`, which does not see through
      # `functools.wraps`.
      @functools.wraps(func)
      async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
        # A new scope per call, so that recursive and concurrent calls do not
        # share state.
        with framework_label(self._label):
          return await func(*args, **kwargs)

      return cast(_CallableT, async_wrapper)

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
      with framework_label(self._label):
        return func(*args, **kwargs)

    return cast(_CallableT, wrapper)
