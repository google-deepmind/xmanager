"""Tests for the alembic database migrations."""

import os
import tempfile

from absl.testing import absltest
from alembic import command
from alembic.config import Config
import sqlalchemy


class AlembicMigrationTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.temp_db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    self.temp_db_file.close()
    self.db_url = f'sqlite:///{self.temp_db_file.name}'

    storage_dir = os.path.dirname(__file__)
    self.alembic_cfg = Config(os.path.join(storage_dir, 'alembic.ini'))
    self.alembic_cfg.set_main_option(
        'script_location', os.path.join(storage_dir, 'alembic')
    )
    self.alembic_cfg.set_main_option('sqlalchemy.url', self.db_url)

    self.engine = sqlalchemy.create_engine(self.db_url)

  def tearDown(self):
    os.unlink(self.temp_db_file.name)
    super().tearDown()

  def test_upgrade_creates_schema(self):
    with self.engine.connect() as connection:
      inspector = sqlalchemy.inspect(connection)
      self.assertEqual(inspector.get_table_names(), [])

    command.upgrade(self.alembic_cfg, 'head')

    with self.engine.connect() as connection:
      inspector = sqlalchemy.inspect(connection)
      expected_tables = ['experiment', 'work_unit', 'job']
      actual_tables = inspector.get_table_names()

      for table in expected_tables:
        self.assertIn(
            table, actual_tables, f"Table '{table}' not found after upgrade."
        )

      experiment_columns = [
          c['name'] for c in inspector.get_columns('experiment')
      ]
      self.assertIn('experiment_id', experiment_columns)
      self.assertIn('experiment_title', experiment_columns)

  def test_downgrade_is_unsupported(self):
    command.upgrade(self.alembic_cfg, 'head')
    # Downgrades are intentionally not implemented; the only migration raises
    # when asked to go below its initial revision.
    with self.assertRaises(RuntimeError):
      command.downgrade(self.alembic_cfg, 'base')


if __name__ == '__main__':
  absltest.main()
