"""Tests for xmanager.xm_local.storage.database."""

import unittest

from google.protobuf import text_format
from xmanager.generated import data_pb2
from xmanager.xm_local.storage import database


class DatabaseTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    # Use an in-memory SQLite database for testing.
    # The Database class will automatically create the schema.
    settings = database.sqlite_settings(db_file=':memory:')
    self.db = database.Database(database.SqliteConnector, settings)

  def test_insert_and_get_vertex_job(self):
    experiment_id = 1
    work_unit_id = 1
    vertex_job_id = 'projects/p/locations/l/customJobs/123'

    self.db.insert_experiment(experiment_id, 'test_experiment')
    self.db.insert_work_unit(experiment_id, work_unit_id)

    self.db.insert_vertex_job(experiment_id, work_unit_id, vertex_job_id)

    work_unit = self.db.get_work_unit(experiment_id, work_unit_id)
    self.assertIn(vertex_job_id, work_unit.jobs)

    retrieved_job = work_unit.jobs[vertex_job_id]
    self.assertEqual(retrieved_job.caip.resource_name, vertex_job_id)

    expected_job = data_pb2.Job()
    text_format.Parse(
        f'caip: {{ resource_name: "{vertex_job_id}" }}', expected_job
    )
    self.assertEqual(retrieved_job, expected_job)


if __name__ == '__main__':
  unittest.main()
