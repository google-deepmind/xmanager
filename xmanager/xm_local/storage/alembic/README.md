# Database Migration

## Alembic

XManager updates may introduce updates to the schema of the database
used to store experiment metadata. Therefore, the existing database must adapt
to the new changes. This is achieved by using
[`alembic`](https://alembic.sqlalchemy.org/en/latest/) to perform migrations.

XManager comes with an already configured `alembic` environment in the
`xm_local.storage` package. Migration scripts are provided in the
`alembic/versions` directory present there.

A new revision can be created using the `alembic revision` command:

```
$ cd xmanager/xm_local/storage
$ alembic -m revision "Create Example table."
  Generating ${PATH_TO_XMANAGER}/xm_local/storage/alembic/versions/106c21f078d5_create_example_table.py ...  done
```

A new file `106c21f078d5_create_example_table.py` is generated. This file
contains details about the migration, like the revision ID `106c21f078d5` and
the `upgrade` and `downgrade` functions. One has to populate these functions
with directives that will apply changes to the database.

```py
"""Create Example table.

Revision ID: 106c21f078d5
Revises: 5cbe03fe7ed1
Create Date: 2022-09-20 09:23:48.029756

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '106c21f078d5'
down_revision = '5cbe03fe7ed1'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
```

We can add some directives to our script, e.g. adding a new table `Example`:

```py
def upgrade() -> None:
  op.create_table(
      'Example',
      sa.Column('Id', sa.Integer, primary_key=True)
  )

def downgrade() -> None:
  op.drop_table('Example')
```

Launching XManager now using the `--xm_upgrade_db` flag will attempt to update
the database. For example, running

```sh
$ xmanager launch examples/cifar10_tensorflow/launcher.py -- --xm_upgrade_db
```
will perform the update and some information about this will be printed:

```
INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade 5cbe03fe7ed1-> 106c21f078d5', Create Example table.
```

For more details on how `alembic` works and how it can be used, check out the
[tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

If more developers work on migrations, make sure to sync with the latest
version of the code, check if there are multiple heads / any issues with the
`alembic` history and create a merge revision accordingly. Check
[this tutorial](https://alembic.sqlalchemy.org/en/latest/branches.html#working-with-branches)
for more details on branching.

NOTE: Migrations created by the user are not explicitly supported by XManager.
On update, it's the responsibility of the user to adapt to new revisions.

## Automatic Migration on Update

If XManager is updated and a new version of the database is available (a new
migration script is provided), the currently used database
(local SQLite or specified through the YAML config file) must be upgraded
to run XManager.

When using `--xm_upgrade_db`, XManager attempts to update the database
to the latest version. Taking a backup of the database
before using this flag is recommended, since migrations may fail.

This doesn't apply for the case when a new database is used.
The initial tables will be created automatically for a new database without
having to use the flag.

NOTE: Making changes to the database from outside of the XManager client
is not supported and discouraged (SQL queries, `alembic` upgrades/downgrades).
All database operations should be performed
automatically by the client.