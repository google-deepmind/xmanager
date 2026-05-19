"""Tests for xmanager.xm_local.storage.database."""

import unittest
from unittest import mock
import os
import tempfile
from xmanager.xm_local.storage import database as db_module
from xmanager.xm_local import experiment as local_experiment

class DatabaseTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, 'db.sqlite')

        settings = db_module.SqlConnectionSettings(backend='sqlite', db_name=self.db_path)

        self.database = db_module.Database(db_module.SqliteConnector, settings)

        self.patcher = mock.patch('xmanager.xm_local.experiment.database.database', return_value=self.database)
        self.mock_db = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.temp_dir.cleanup()

    def test_create_experiment(self):
        with local_experiment.create_experiment(experiment_title='test_experiment_1') as experiment:
            self.assertIsNotNone(experiment.experiment_id)

            with self.database.engine.connect() as connection:
                result = connection.execute(db_module.text("SELECT * FROM experiment"))
                rows = result.all()
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0].experiment_title, 'test_experiment_1')

        with local_experiment.create_experiment(experiment_title='test_experiment_2') as experiment:
            self.assertIsNotNone(experiment.experiment_id)

            with self.database.engine.connect() as connection:
                result = connection.execute(db_module.text("SELECT * FROM experiment"))
                rows = result.all()
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[1].experiment_title, 'test_experiment_2')

if __name__ == '__main__':
    unittest.main()
