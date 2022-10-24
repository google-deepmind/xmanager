# Metadata Storage

Experiment metadata is stored in a SQL database. By default, the database used
is the SQLite one at `~/.xmanager/experiments.sqlite3`. If that does not
suffice, the XManager client can also connect to a generic database based on a
YAML configuration. An example of such a configuration is given below:

```yaml
# sample_db_config.yaml

# Connector used - one of ['cloudsql', 'sqlite', 'generic']
sql_connector: 'generic'

sql_connection_settings:
  # Backend used, e.g. 'mysql', 'postgresql'
  backend: 'mysql'
  # Driver used, e.g. 'pymysql', 'pg8000'
  driver: 'pymysql'
  # Username
  username: 'root'
  # Password
  password: 'metadata'
  # Host (or instance connection name for CloudSql)
  host: '127.0.01'
  # Port
  port: 3309
  # Database name
  db_name: 'metadata'
```

The `sql_connector` field specifies the connector to be used. It can be one of
`sqlite`, `cloudsql`, `generic`. Generally, it's recommended to use the `sqlite`
connector for a local DB at a different location than the default one,
`cloudsql` for connecting to a CloudSQL database, and `generic` for any other
database.

The `cloudsql` connector runs the
[CloudSQL Auth Proxy](https://cloud.google.com/sql/docs/mysql/sql-proxy)
automatically, so it requires the host that runs the XManager client to have the
required permissions (Cloud SQL Editor IAM role) and project APIs enabled (Cloud
SQL Admin API). When using the `cloudsql` connector, one should use an instance
connection name of the format
`${PROJECT_ID}:${INSTANCE_LOCATION}:${INSTANCE_NAME}` in the `host` field of the
YAML configuration.

When using the `generic` connector, the fields in the `sql_connection_settings`
follow the format of the `sqlalchemy` connection URL. Therefore, each
combination of the fields above that can form a valid `sqlalchemy` connection
URL can be used.

This YAML config can be passed to XManager by using the
`--xm_db_yaml_config_path` flag. Note that the path is relative to the launch
script. If the flag is not enabled, the default SQLite database is used.

The XManager client attempts to always keep the database to the latest version.
For details, check
[database migration docs](https://github.com/deepmind/xmanager/tree/main/xmanager/xm_local/storage/alembic/README.md).

## Slides

Remote metadata execution and metadata storage are present in
[these slides](https://storage.googleapis.com/gresearch/xmanager/remote_execution_slides.pdf).
