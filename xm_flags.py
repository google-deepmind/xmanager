# Copyright 2024 DeepMind Technologies Limited
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
"""XManager Flags."""

import enum
from absl import flags

# -------------------- xm_local --------------------

DB_YAML_CONFIG_PATH = flags.DEFINE_string(
    'xm_db_yaml_config_path',
    None,
    """
    Path of YAML config file containing DB connection details.

    A valid config file contains two main entries:
      `sql_connector`: must be one of [`sqlite`, `generic`, `cloudsql`]

      `sql_connection_settings`: contains details about the connection URL.
        These match the interface of `SqlConnectionSettings` and their
        combination must form a valid `sqlalchemy` connection URL. Possible
        fields are:
          - backend, e.g. 'mysql', 'postgresql'
          - db_name
          - driver, e.g. 'pymysql', 'pg8000'
          - username
          - password
          - host (instance connection name when using CloudSql)
          - port
    """,
)

UPGRADE_DB = flags.DEFINE_boolean(
    'xm_upgrade_db',
    False,
    """
    Specifies if XManager should update the database to the latest version.
    It's recommended to take a back-up of the database before updating, since
    migrations can fail/have errors. This is especially true
    for non-transactional DDLs, where partial migrations can occur on
    failure, leaving the database in a not well-defined state.
    """,
)

BAZEL_COMMAND = flags.DEFINE_string(
    'xm_bazel_command', 'bazel', 'A command that runs Bazel.'
)

# -------------------- contrib --------------------

GCS_PATH = flags.DEFINE_string(
    'xm_gcs_path',
    None,
    (
        'A GCS directory within a bucket to store output '
        '(in gs://bucket/directory format).'
    ),
)


class XMLaunchMode(enum.Enum):
  """Specifies an executor to run an experiment."""

  VERTEX = 'vertex'
  LOCAL = 'local'
  INTERACTIVE = 'interactive'


XM_LAUNCH_MODE = flags.DEFINE_enum_class(
    'xm_launch_mode',
    XMLaunchMode.VERTEX,
    XMLaunchMode,
    'How to launch the experiment. Supports local and interactive execution, '
    + 'launch on '
    +
    'Vertex.',
)
