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
"""Unit tests for database transactions and job serialization."""

import os
import tempfile
from unittest import mock

from absl import flags
from xmanager.xm_local import experiment as local_experiment
from xmanager.xm_local.storage import database as db_module

from absl.testing import absltest as googletest


class DatabaseTest(googletest.TestCase):

  def setUp(self):
    super().setUp()
    if not flags.FLAGS.is_parsed():
      flags.FLAGS.mark_as_parsed()
    self.temp_dir = tempfile.TemporaryDirectory()
    self.db_path = os.path.join(self.temp_dir.name, 'db.sqlite')

    settings = db_module.SqlConnectionSettings(
        backend='sqlite', db_name=self.db_path
    )

    self.database = db_module.Database(db_module.SqliteConnector, settings)

    self.patcher = mock.patch(
        'xmanager.xm_local.experiment.database.database',
        return_value=self.database,
    )
    self.mock_db = self.patcher.start()

  def tearDown(self):
    self.patcher.stop()
    self.temp_dir.cleanup()
    super().tearDown()

  def test_create_experiment(self):
    with local_experiment.create_experiment(
        experiment_title='test_experiment_1'
    ) as experiment:
      self.assertIsNotNone(experiment.experiment_id)

      with self.database.engine.connect() as connection:
        result = connection.execute(db_module.text('SELECT * FROM experiment'))
        rows = result.all()
        self.assertLen(rows, 1)
        self.assertEqual(rows[0].experiment_title, 'test_experiment_1')

    with local_experiment.create_experiment(
        experiment_title='test_experiment_2'
    ) as experiment:
      self.assertIsNotNone(experiment.experiment_id)

      with self.database.engine.connect() as connection:
        result = connection.execute(db_module.text('SELECT * FROM experiment'))
        rows = result.all()
        self.assertLen(rows, 2)
        self.assertEqual(rows[1].experiment_title, 'test_experiment_2')

  def test_job_data_roundtrip(self):
    self.database.insert_experiment(1, 'test_experiment')
    self.database.insert_work_unit(1, 1)
    self.database.insert_vertex_job(
        1, 1, 'projects/p/locations/l/customJobs/123'
    )
    self.database.insert_kubernetes_job(1, 1, 'test_namespace', 'k8s_job')

    work_unit = self.database.get_work_unit(1, 1)

    self.assertCountEqual(
        work_unit.jobs.keys(),
        ['projects/p/locations/l/customJobs/123', 'k8s_job'],
    )

    vertex_job = work_unit.jobs['projects/p/locations/l/customJobs/123']
    self.assertEqual(
        vertex_job.caip.resource_name, 'projects/p/locations/l/customJobs/123'
    )

    kubernetes_job = work_unit.jobs['k8s_job']
    self.assertEqual(kubernetes_job.kubernetes.namespace, 'test_namespace')
    self.assertEqual(kubernetes_job.kubernetes.job_name, 'k8s_job')


if __name__ == '__main__':
  googletest.main()
