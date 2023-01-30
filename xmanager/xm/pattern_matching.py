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
"""Pattern matching implementation.

Until Python 3.10 with PEP 646 is available to us, this module provides an
alternative implementation of https://en.wikipedia.org/wiki/Pattern_matching.

This module is private and can only be used by the API itself, as the provided
API is not final.
"""

import inspect
from typing import Any, Awaitable, Callable, Iterable, Generic, Tuple, Type, TypeVar, Union

R = TypeVar('R')


class Case(Generic[R]):
  """A handler of a particular type branch."""

  # A tuple of types to match. One for each callable argument.
  # Immutable, should not be modified.
  kind: Tuple[Type[Any]]
  # A handler to be called in case of a match.
  # Must be `Callable[..., R]`. Immutable, should not be modified.
  handle: Any

  def __init__(
      self,
      kind: Iterable[Type[Any]],
      handler: Callable[..., R],
  ) -> None:
    """Constructs a handler for given types.

    Args:
      kind: A tuple of types to match. One for each callable argument.
      handler: A handler to be called in case of a match.
    """
    self.kind = tuple(
        getattr(arg_type, '__origin__', arg_type) for arg_type in kind
    )
    self.handle = handler

  def matches(self, *values: Any) -> bool:
    """Checks if *values match the types."""
    if len(values) != len(self.kind):
      return False

    return all(
        expected_type is Any or isinstance(value, expected_type)
        for expected_type, value in zip(self.kind, values)
    )


def _deduce_types(handler: Callable[..., Any]) -> Tuple[Type[Any]]:
  """Returns callable's argument types."""
  argspec = inspect.getfullargspec(handler)

  def get_arg_type(arg: str):
    """Returns argument type or raises a user-friendly exception."""
    if arg not in argspec.annotations:
      raise ValueError(
          f'{arg} argument of a {handler} is missing a type annotation.'
      )
    return argspec.annotations[arg]

  return tuple(map(get_arg_type, argspec.args))


def match(*handlers: Union[Case[R], Callable[..., R]]) -> Callable[..., R]:
  """Creates a router applying appropriate function based on value's type.

  This is an implementation of https://en.wikipedia.org/wiki/Pattern_matching.

  Args:
    *handlers: Sequence of branches.

  Returns:
    A function that selects the earliest case whose arguments types are equal or
    base to the given value's type and applies its handler. If none of the cases
    match, a `TypeError` is raised.

  Examples:

    ```
    def visit_str(s: str):
      return reversed(s)

    matcher = match(
      visit_str,
      Case([int], lambda i: str(i)),
    )
    matcher(0)
    ```
  """

  cases = [
      handler
      if isinstance(handler, Case)
      else Case(_deduce_types(handler), handler)
      for handler in handlers
  ]

  def apply(*values: Any) -> R:
    for case in cases:
      if case.matches(*values):
        return case.handle(*values)

    value_types = tuple(type(value) for value in values)
    known_types = '\n'.join(str(case.kind) for case in cases)
    raise TypeError(
        f'{values} did not match any type pattern. Values have '
        'following types:\n'
        f'{value_types}\n'
        'Which does not match any of:\n'
        f'{known_types}'
    )

  return apply


def async_match(*handlers: Callable[..., R]) -> Callable[..., Awaitable[R]]:
  """`match` for async functions."""
  # Python type system doesn't differentiate regular and async functions enough.
  # `async def f(int) -> Cls:` is a `Callable[[int], Cls]` but actually returns
  # `Awaitable[Cls]`. This confuses pytype when it deduces `match` return type.

  return match(*handlers)
