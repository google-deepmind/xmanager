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


def using_legacy_sqlite_db():
  connection = op.get_bind()
  return 'VersionHistory' in Inspector.from_engine(connection).get_table_names()


def create_new_tables():
  op.create_table('Experiment', sa.Column('Id', sa.Integer(), primary_key=True),
                  sa.Column('Title', sa.String(255)))

  op.create_table('WorkUnit',
                  sa.Column('ExperimentId', sa.Integer(), primary_key=True),
                  sa.Column('WorkUnitId', sa.Integer(), primary_key=True))

  op.create_table('Job',
                  sa.Column('ExperimentId', sa.Integer(), primary_key=True),
                  sa.Column('WorkUnitId', sa.Integer(), primary_key=True),
                  sa.Column('Name', sa.String(255), primary_key=True),
                  sa.Column('Data', sa.String(255)))


def upgrade() -> None:
  """Upgrades DB."""
  if using_legacy_sqlite_db():
    op.drop_table('VersionHistory')
  else:
    create_new_tables()


def downgrade() -> None:
  """Downgrades DB."""
  raise RuntimeError('Downgrade operation is not supported: would downgrade '
                     'to legacy SQLite schema using `VersionHistory` or to '
                     ' empty database.')
