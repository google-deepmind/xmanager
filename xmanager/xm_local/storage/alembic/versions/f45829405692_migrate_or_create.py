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
#
# Generated script names don't follow the module naming convention.
# pylint: disable=invalid-name
"""Migrate from legacy SQLite DB or create new tables.

There are two ways a DB doesn't already have a current revision:
  1. It's an old SQLite DB using the `VersionHistory` table
    => Migrate to new schema.

  2. It's a new database with no tables.
    => Create tables

Revision ID: f45829405692
Revises:
Create Date: 2022-09-16 10:50:41.096403

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision = 'f45829405692'
down_revision = None
branch_labels = None
depends_on = None


def using_legacy_sqlite_db() -> bool:
  connection = op.get_bind()
  return 'VersionHistory' in Inspector.from_engine(connection).get_table_names()


def update_columns() -> None:
  """Migrates legacy SQLite DB to portable column names and types.

  `batch_alter_table` is required when using SQLite because of its limited
  support for the `ALTER` statement (see
  https://alembic.sqlalchemy.org/en/latest/batch.html)

  SQLite table names are case-insensitive, but the table names
  still appear to be upper case when inspecting database with `sqlite`.
  One has to rename to an intermediate name when changing the case of a
  table name is required.
  """
  with op.batch_alter_table('Experiment') as batch_op:
    batch_op.alter_column(
        column_name='Id',
        new_column_name='experiment_id',
        existing_type=sa.Integer(),
        type_=sa.BigInteger())

    batch_op.alter_column(
        column_name='Title',
        new_column_name='experiment_title',
        existing_type=sa.TEXT,
        type_=sa.String(255))
  op.rename_table('Experiment', 'tmp_experiment')
  op.rename_table('tmp_experiment', 'experiment')

  with op.batch_alter_table('WorkUnit') as batch_op:
    batch_op.alter_column(
        column_name='ExperimentId',
        new_column_name='experiment_id',
        existing_type=sa.Integer(),
        type_=sa.BigInteger())

    batch_op.alter_column(
        column_name='WorkUnitId',
        new_column_name='work_unit_id',
    )
  op.rename_table('WorkUnit', 'work_unit')

  with op.batch_alter_table('Job') as batch_op:
    batch_op.alter_column(
        column_name='ExperimentId',
        new_column_name='experiment_id',
        existing_type=sa.Integer(),
        type_=sa.BigInteger())

    batch_op.alter_column(
        column_name='WorkUnitId',
        new_column_name='work_unit_id',
    )

    batch_op.alter_column(
        column_name='Name',
        new_column_name='job_name',
        existing_type=sa.TEXT,
        type_=sa.String(255))

    batch_op.alter_column(
        column_name='Data',
        new_column_name='job_data',
        existing_type=sa.TEXT,
        type_=sa.String(255))
  op.rename_table('Job', 'tmp_job')
  op.rename_table('tmp_job', 'job')


def create_new_tables() -> None:
  """Creates tables for new database.

  `autoincrement` field is required for MSSql (check
  https://docs.sqlalchemy.org/en/13/dialects/mssql.html#auto-increment-behavior-identity-columns)
  """
  op.create_table(
      'experiment',
      sa.Column(
          'experiment_id',
          sa.BigInteger(),
          primary_key=True,
          autoincrement=False), sa.Column('experiment_title', sa.String(255)))

  op.create_table(
      'work_unit',
      sa.Column(
          'experiment_id',
          sa.BigInteger(),
          primary_key=True,
          autoincrement=False),
      sa.Column('work_unit_id', sa.Integer(), primary_key=True))

  op.create_table(
      'job',
      sa.Column(
          'experiment_id',
          sa.BigInteger(),
          primary_key=True,
          autoincrement=False),
      sa.Column('work_unit_id', sa.Integer(), primary_key=True),
      sa.Column('job_name', sa.String(255), primary_key=True),
      sa.Column('job_data', sa.String(255)))


def upgrade() -> None:
  """Upgrades DB."""
  if using_legacy_sqlite_db():
    op.drop_table('VersionHistory')

    update_columns()
  else:
    create_new_tables()


def downgrade() -> None:
  """Downgrades DB."""
  raise RuntimeError('Downgrade operation is not supported: would downgrade '
                     'to legacy SQLite schema using `VersionHistory` or to '
                     ' empty database.')
