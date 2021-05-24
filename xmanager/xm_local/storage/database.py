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

import attr
import sqlalchemy
import sqlite3

Engine = sqlalchemy.engine.Engine


class SqlSettings(abc.ABC):
  """Settings for a SQL dialect."""

  @abc.abstractmethod
  def create_engine(self) -> Engine:
    raise NotImplementedError

  @abc.abstractmethod
  def execute_script(self, script: str) -> None:
    raise NotImplementedError


@attr.s(auto_attribs=True)
class SqliteSettings(SqlSettings):
  """Settings for the Sqlite dialect."""

  path: str = os.path.expanduser('~/.xmanager/experiments.sqlite3')

  def create_engine(self) -> Engine:
    if not os.path.isdir(os.path.dirname(self.path)):
      os.makedirs(os.path.dirname(self.path))
    if not os.path.isfile(self.path):
      sqlite3.connect(self.path)
    return sqlalchemy.create_engine(f'sqlite:///{self.path}')

  def execute_script(self, script: str) -> None:
    with open(script) as f:
      content = f.read()
    conn = sqlite3.connect(self.path)
    cursor = conn.cursor()
    cursor.executescript(content)


@functools.lru_cache()
def database():
  # Create only a single global singleton for database access.
  return Database()


class Database:
  """Database object with interacting with experiment metadata storage."""

  def __init__(self, settings: SqlSettings = SqliteSettings()):
    self.settings = settings
    self.engine: Engine = settings.create_engine()
    self.maybe_migrate_database_version(self.engine)

  def maybe_migrate_database_version(self, engine: Engine):
    """Check the database VersionHistory table and maybe migrate."""
    # Create the tables for the first time.
    if 'VersionHistory' not in engine.table_names():
      schema = os.path.join(
          os.path.dirname(os.path.realpath(__file__)), 'schema.sql')
      self.settings.execute_script(schema)

    with engine.connect() as conn:
      rows = list(
          conn.execute(
              'SELECT Version, Timestamp FROM VersionHistory ORDER BY Timestamp DESC LIMIT 1'
          ))
      if not rows:
        raise ValueError('The database is invalid. It has no VersionHistory.')
      if rows[0][0] > 1:
        raise ValueError(
            f'The database schema is on an unsupported version: {rows[0][0]}')
