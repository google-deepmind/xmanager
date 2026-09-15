"""No-op database for environments where storage is not used."""

import functools
from typing import Any, Sequence


class NoopDatabase:
  """A no-op database implementing the `database.Database` interface."""

  def insert_experiment(
      self, experiment_id: int, experiment_title: str
  ) -> None:
    pass

  def insert_work_unit(self, experiment_id: int, work_unit_id: int) -> None:
    pass

  def insert_vertex_job(
      self, experiment_id: int, work_unit_id: int, vertex_job_id: str
  ) -> None:
    pass

  def insert_kubernetes_job(
      self,
      experiment_id: int,
      work_unit_id: int,
      namespace: str,
      job_name: str,
  ) -> None:
    pass

  def list_experiment_ids(self) -> Sequence[int]:
    raise NotImplementedError(
        'Listing local experiments from storage is not supported.'
    )

  def get_experiment(self, experiment_id: int) -> Any:
    del experiment_id
    raise NotImplementedError(
        'Loading local experiments from storage is not supported.'
    )

  def list_work_units(self, experiment_id: int) -> Sequence[Any]:
    del experiment_id
    raise NotImplementedError(
        'Loading local experiments from storage is not supported.'
    )

  def get_work_unit(self, experiment_id: int, work_unit_id: int) -> Any:
    del experiment_id, work_unit_id
    raise NotImplementedError(
        'Loading local experiments from storage is not supported.'
    )


@functools.lru_cache()
def database() -> NoopDatabase:
  """Returns a singleton no-op database instance."""
  return NoopDatabase()
