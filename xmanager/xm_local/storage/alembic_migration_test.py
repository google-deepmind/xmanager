import unittest
import tempfile
import os
import sqlalchemy
from sqlalchemy import inspect
from alembic.config import Config
from alembic import command
from alembic import script as alembic_script

class AlembicMigrationTest(unittest.TestCase):

    def setUp(self):
        self.temp_db_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db_file.close()
        self.db_url = f"sqlite:///{self.temp_db_file.name}"

        alembic_dir = os.path.join(
            os.path.dirname(__file__), 'alembic'
        )
        self.alembic_cfg = Config(os.path.join(alembic_dir, 'alembic.ini'))
        self.alembic_cfg.set_main_option("script_location", alembic_dir)
        self.alembic_cfg.set_main_option("sqlalchemy.url", self.db_url)

        self.engine = sqlalchemy.create_engine(self.db_url)

    def tearDown(self):
        os.unlink(self.temp_db_file.name)

    def test_migrations_upgrade_and_downgrade(self):
        with self.engine.connect() as connection:
            inspector = inspect(connection)
            self.assertEqual(inspector.get_table_names(), [])

        print(f"Upgrading database to head: {self.db_url}")
        command.upgrade(self.alembic_cfg, "head")

        # Verify schema after upgrade
        with self.engine.connect() as connection:
            inspector = inspect(connection)
            expected_tables = ['experiment', 'work_unit', 'job'] # Example tables from your existing migration
            actual_tables = inspector.get_table_names()

            self.assertGreaterEqual(len(actual_tables), len(expected_tables),
                                    "Not all expected tables were created during upgrade.")
            for table in expected_tables:
                self.assertIn(table, actual_tables, f"Table '{table}' not found after upgrade.")

            experiment_columns = [c['name'] for c in inspector.get_columns('experiment')]
            self.assertIn('experiment_id', experiment_columns)
            self.assertIn('experiment_title', experiment_columns)

        print(f"Attempting to downgrade database to base: {self.db_url}")
        with self.assertRaises(RuntimeError):
            command.downgrade(self.alembic_cfg, "base")

if __name__ == '__main__':
    unittest.main()
