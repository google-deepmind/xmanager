"""Registry of execution logic for each executor type.

Execution logic is automatically registered when the corresponding Executor
class is instantiated.

The registry allows users to avoid heavy dependencies for executors that are not
used in the experiment (i.e. heavy Kubernetes deps for the `xm_local.Kubernetes`
executor).
"""

from typing import Awaitable, Callable, Generic, Sequence, Type, TypeVar

import attr
from xmanager import xm
from xmanager.xm_local import handles


_Handle = TypeVar('_Handle', bound=handles.ExecutionHandle)


@attr.s(auto_attribs=True)
class _ExecutorInfo(Generic[_Handle]):
  # Method to launch a job group and get the execution handles.
  launch: Callable[..., Awaitable[Sequence[_Handle]]]
  # Method to create an execution handle using data from the local database.
  create_handle: Callable[..., _Handle] | None = None


_REGISTRY: dict[Type[xm.Executor], _ExecutorInfo] = {}


def register(
    executor_type: Type[xm.Executor],
    launch: Callable[..., Awaitable[Sequence[_Handle]]],
    create_handle: Callable[..., _Handle] | None = None,
):
  _REGISTRY[executor_type] = _ExecutorInfo(
      launch=launch, create_handle=create_handle
  )


def is_registered(executor_type: Type[xm.Executor]) -> bool:
  return executor_type in _REGISTRY


def get_launch_method(
    executor_type: Type[xm.Executor],
) -> Callable[..., Awaitable[Sequence[_Handle]]]:
  return _REGISTRY[executor_type].launch


def get_create_handle_method(
    executor_type: Type[xm.Executor],
) -> Callable[..., _Handle] | None:
  return _REGISTRY[executor_type].create_handle
