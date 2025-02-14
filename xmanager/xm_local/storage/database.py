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
"""Database connector module."""
import abc
import functools
import os
import tempfile
from typing import Any, Dict, List, Optional, Type, TypeVar

from alembic import command
from alembic import migration
from alembic import script as alembic_script
from alembic.config import Config
import attr
import sqlalchemy
from xmanager import xm
from xmanager import xm_flags
from xmanager.generated import data_pb2
import yaml

from google.protobuf import text_format

from google.cloud.sql.connector import Connector, IPTypes


@attr.s(auto_attribs=True)
class WorkUnitResult:
  """Result of a WorkUnit database query."""

  work_unit_id: int
  jobs: Dict[str, data_pb2.Job]


@attr.s(auto_attribs=True)
class ExperimentResult:
  """Result of an Experiment database query."""

  experiment_id: int
  experiment_title: str
  work_units: List[WorkUnitResult]


Engine = sqlalchemy.engine.Engine
text = sqlalchemy.sql.text


@attr.s(auto_attribs=True)
class SqlConnectionSettings:
  """Settings for a generic SQL connection."""

  backend: str
  db_name: str
  driver: Optional[str] = None
  username: Optional[str] = None
  password: Optional[str] = None
  host: Optional[str] = None
  port: Optional[int] = None


class SqlConnector(abc.ABC):
  """Provides a way of connecting to a SQL DB from some settings."""

  @staticmethod
  @abc.abstractmethod
  def create_engine(settings: SqlConnectionSettings) -> Engine:
    raise NotImplementedError


TSqlConnector = TypeVar('TSqlConnector', bound=SqlConnector)


class GenericSqlConnector(SqlConnector):
  """Generic way of connecting to SQL databases using an URL."""

  @staticmethod
  def create_engine(settings: SqlConnectionSettings) -> Engine:
    driver_name = settings.backend + (
        f'+{settings.driver}' if settings.driver else ''
    )

    url = sqlalchemy.engine.url.URL(
        drivername=driver_name,
        username=settings.username,
        password=settings.password,
        host=settings.host,
        port=settings.port,
        database=settings.db_name,
    )

    return sqlalchemy.engine.create_engine(url)


class SqliteConnector(SqlConnector):
  """Provides way of connecting to a SQLite database.

  The database used at the file path pointed to by the `database` field
  in the settings used. The database is created if it doesn't exist.
  """

  @staticmethod
  def create_engine(settings: SqlConnectionSettings) -> Engine:
    if settings.backend and settings.backend != 'sqlite':
      raise RuntimeError(
          "Can't use SqliteConnector with a backendother than `sqlite`"
      )

    if not os.path.isdir(os.path.dirname(settings.db_name)):
      os.makedirs(os.path.dirname(settings.db_name))

    return GenericSqlConnector.create_engine(settings)


class CloudSqlConnector(SqlConnector):
  """Provides way of connecting to a CloudSQL database."""

  # Each CloudSql backend supports one driver.
  BACKEND_DRIVERS = {
      'mysql': 'pymysql',
      'postgresql': 'pg8000',
      'mssql': 'pytds'
  }

  @staticmethod
  def create_engine(settings: SqlConnectionSettings) -> Engine:
    ip_type = IPTypes.PRIVATE if os.environ.get(
        'PRIVATE_IP') else IPTypes.PUBLIC
    connector = Connector(ip_type)

    if settings.backend not in CloudSqlConnector.BACKEND_DRIVERS:
      raise RuntimeError(f'CloudSql doesn\'t support the '
                         f'`{settings.backend}` backend.')

    driver = CloudSqlConnector.BACKEND_DRIVERS[settings.backend]
    if settings.driver and settings.driver != driver:
      raise RuntimeError(f'CloudSql backend `{settings.backend}` does not '
                         f'support the `{settings.driver}` driver')

    def get_connection():
      return connector.connect(
          settings.host,
          driver,
          user=settings.username,
          password=settings.password,
          db=settings.db_name)

    url = sqlalchemy.engine.url.URL(drivername=f'{settings.backend}+{driver}',
                                    host='localhost')
    return sqlalchemy.create_engine(url, creator=get_connection)


class Database:
  """Database object with interacting with experiment metadata storage."""

  def __init__(
      self, connector: Type[TSqlConnector], settings: SqlConnectionSettings
  ):
    self.settings = settings
    self.engine: Engine = connector.create_engine(settings)
    # https://github.com/sqlalchemy/sqlalchemy/issues/5645
    # TODO: Remove this line after using sqlalchemy>=1.14.
    self.engine.dialect.description_encoding = None
    storage_dir = os.path.dirname(__file__)

    alembic_ini_path = os.path.join(storage_dir, 'alembic.ini')

    self.alembic_cfg = Config(alembic_ini_path)

    self.alembic_cfg.set_main_option('sqlalchemy.url', str(self.engine.url))

    alembic_script_path = os.path.join(storage_dir, 'alembic')

    self.alembic_cfg.set_main_option('script_location', alembic_script_path)

    self.maybe_migrate_database_version()

  def upgrade_database(self, revision: str = 'head') -> None:
    """Upgrades database to given revision."""
    with self.engine.begin() as connection:
      # https://alembic.sqlalchemy.org/en/latest/cookbook.html#sharing-a-connection-across-one-or-more-programmatic-migration-commands
      # Allows sharing connection across multiple commands.
      self.alembic_cfg.attributes['connection'] = connection
      try:
        command.upgrade(self.alembic_cfg, revision)
      except Exception as e:
        raise RuntimeError(
            'Database upgrade failed. The DB may be in an undefined state or '
            'data may have been lost. Revert to the previous state using your '
            'backup or proceed with caution.'
        ) from e
      finally:
        self.alembic_cfg.attributes['connection'] = None

  def database_version(self) -> Optional[str]:
    with self.engine.begin() as connection:
      context = migration.MigrationContext.configure(connection)
      return context.get_current_revision()

  def latest_version_available(self) -> Optional[str]:
    script_directory = alembic_script.ScriptDirectory.from_config(
        self.alembic_cfg
    )
    return script_directory.get_current_head()

  def maybe_migrate_database_version(self):
    """Enforces the latest version of the database to be used."""
    db_version = self.database_version()
    with self.engine.connect() as connection:
      legacy_sqlite_db = self.engine.dialect.has_table(
          connection, 'VersionHistory'
      )

    need_to_update = (
        db_version != self.latest_version_available() and db_version
    ) or legacy_sqlite_db
    if need_to_update and not xm_flags.UPGRADE_DB.value:
      raise RuntimeError(
          f'Database is not up to date: current={self.database_version()}, '
          f'latest={self.latest_version_available()}. Take a backup of the '
          'database and then launch using the `--xm_upgrade_db` flag '
          'to update to the the latest version.'
      )
    self.upgrade_database()

  def insert_experiment(
      self, experiment_id: int, experiment_title: str
  ) -> None:
    query = text(
        'INSERT INTO experiment (experiment_id, experiment_title) '
        'VALUES (:experiment_id, :experiment_title)'
    )
    self.engine.execute(
        query, experiment_id=experiment_id, experiment_title=experiment_title
    )

  def insert_work_unit(self, experiment_id: int, work_unit_id: int) -> None:
    query = text(
        'INSERT INTO work_unit (experiment_id, work_unit_id) '
        'VALUES (:experiment_id, :work_unit_id)'
    )
    self.engine.execute(
        query, experiment_id=experiment_id, work_unit_id=work_unit_id
    )

  def insert_vertex_job(
      self, experiment_id: int, work_unit_id: int, vertex_job_id: str
  ) -> None:
    job = data_pb2.Job(caip=data_pb2.AIPlatformJob(resource_name=vertex_job_id))
    data = text_format.MessageToBytes(job)
    query = text(
        'INSERT INTO '
        'job (experiment_id, work_unit_id, job_name, job_data) '
        'VALUES (:experiment_id, :work_unit_id, :job_name, :job_data)'
    )
    self.engine.execute(
        query,
        experiment_id=experiment_id,
        work_unit_id=work_unit_id,
        job_name=vertex_job_id,
        job_data=data,
    )

  def insert_kubernetes_job(
      self, experiment_id: int, work_unit_id: int, namespace: str, job_name: str
  ) -> None:
    """Insert a Kubernetes job into the database."""
    job = data_pb2.Job(
        kubernetes=data_pb2.KubernetesJob(
            namespace=namespace, job_name=job_name
        )
    )
    data = text_format.MessageToString(job)
    query = text(
        'INSERT INTO '
        'job (experiment_id, work_unit_id, job_name, job_data) '
        'VALUES (:experiment_id, :work_unit_id, :job_name, :job_data)'
    )
    self.engine.execute(
        query,
        experiment_id=experiment_id,
        work_unit_id=work_unit_id,
        job_name=job_name,
        job_data=data,
    )

  def list_experiment_ids(self) -> List[int]:
    """Lists all the experiment ids from local database."""
    query = text('SELECT experiment_id FROM experiment')
    rows = self.engine.execute(query)
    return [r['experiment_id'] for r in rows]

  def get_experiment(self, experiment_id: int) -> ExperimentResult:
    """Gets an experiment from local database."""
    query = text(
        'SELECT experiment_title FROM experiment '
        'WHERE experiment_id=:experiment_id'
    )
    rows = self.engine.execute(query, experiment_id=experiment_id)
    title = None
    for r in rows:
      title = r['experiment_title']
      break
    if title is None:
      raise ValueError(f"Experiment Id {experiment_id} doesn't exist.")
    return ExperimentResult(
        experiment_id, title, self.list_work_units(experiment_id)
    )

  def list_work_units(self, experiment_id: int) -> List[WorkUnitResult]:
    """Lists an experiment's work unit ids from local database."""
    query = text(
        'SELECT work_unit_id FROM work_unit WHERE experiment_id=:experiment_id'
    )
    rows = self.engine.execute(query, experiment_id=experiment_id)
    return [self.get_work_unit(experiment_id, r['work_unit_id']) for r in rows]

  def get_work_unit(
      self, experiment_id: int, work_unit_id: int
  ) -> WorkUnitResult:
    """Gets a work unit from local database."""
    query = text(
        'SELECT job_name, job_data FROM job '
        'WHERE experiment_id=:experiment_id '
        'AND work_unit_id=:work_unit_id'
    )
    rows = self.engine.execute(
        query, experiment_id=experiment_id, work_unit_id=work_unit_id
    )
    jobs = {}
    for r in rows:
      job = data_pb2.Job()
      jobs[r['job_name']] = text_format.Parse(r['job_data'], job)
    return WorkUnitResult(work_unit_id, jobs)


def sqlite_settings(
    db_file='~/.xmanager/experiments.sqlite3',
) -> SqlConnectionSettings:
  return SqlConnectionSettings(
      backend='sqlite', db_name=os.path.expanduser(db_file)
  )


_SUPPORTED_CONNECTORS = ['sqlite', 'generic', 'cloudsql']


def _validate_db_config(config: Dict[str, Any]) -> None:
  if 'sql_connector' not in config:
    raise RuntimeError('DB YAML config must contain `sql_connector` entry')
  if config['sql_connector'] not in _SUPPORTED_CONNECTORS:
    raise RuntimeError(
        f'`sql_connector` must be one of: {_SUPPORTED_CONNECTORS}'
    )

  if 'sql_connection_settings' not in config:
    raise RuntimeError(
        'DB YAML config must contain `sql_connection_settings` entry'
    )


@functools.lru_cache()
def _db_config() -> Dict[str, Any]:
  """Parses and validates YAML DB config file to a dict."""
  if xm_flags.DB_YAML_CONFIG_PATH.value is not None:
    db_config_file = xm.utils.resolve_path_relative_to_launcher(
        xm_flags.DB_YAML_CONFIG_PATH.value
    )
    with open(db_config_file, 'r') as f:
      config = yaml.safe_load(f)
      _validate_db_config(config)
      return config

  return {}


@functools.lru_cache()
def db_connector() -> Type[TSqlConnector]:
  """Returns connector based on DB configuration."""
  sql_connector = _db_config().get('sql_connector')

  if sql_connector is None or sql_connector == 'sqlite':
    return SqliteConnector

  if sql_connector == 'cloudsql':
    return CloudSqlConnector

  return GenericSqlConnector


@functools.lru_cache()
def db_settings() -> SqlConnectionSettings:
  """Returns connection settings created based on DB configuration."""
  if _db_config():
    return SqlConnectionSettings(**_db_config()['sql_connection_settings'])

  return sqlite_settings()


@functools.lru_cache()
def database() -> Database:
  """Returns database based on DB configuration."""
  return Database(db_connector(), db_settings())
