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

# --------------------- cloud ---------------------

BUILD_IMAGE_LOCALLY = flags.DEFINE_boolean(
    'xm_build_image_locally',
    True,
    (
        'Use local Docker to build images instead of remote Google Cloud Build.'
        ' This is usually a lot faster but requires docker to be installed.'
    ),
)

USE_DOCKER_COMMAND = flags.DEFINE_boolean(
    'xm_use_docker_command',
    True,
    (
        'Call "docker build" in a subprocess rather than using Python docker '
        'client library when building the docker image locally. This provies a '
        'much nicer output for interactive use.'
    ),
)

SHOW_DOCKER_COMMAND_PROGRESS = flags.DEFINE_boolean(
    'xm_show_docker_command_progress',
    False,
    'Show container output during the "docker build".',
)

WRAP_LATE_BINDINGS = flags.DEFINE_boolean(
    'xm_wrap_late_bindings',
    False,
    (
        'Feature flag to wrap and unwrap late bindings for network addresses. '
        'ONLY works with PythonContainer with default instructions or simple '
        'instructions that do not modify the file directory. '
        'REQUIRES ./entrypoint.sh to be the ENTRYPOINT.'
    ),
)

CLOUD_BUILD_TIMEOUT_SECONDS = flags.DEFINE_integer(
    'xm_cloud_build_timeout_seconds',
    1200,
    (
        'The amount of time that builds should be allowed to run, '
        'to second granularity.'
    ),
)

USE_CLOUD_BUILD_CACHE = flags.DEFINE_boolean(
    'xm_use_cloud_build_cache',
    False,
    (  # pylint:disable=g-line-too-long
        'Use Cloud Build cache to speed up the Docker build. '
        'An image with the same name tagged as :latest should exist.'
        'More details at'
        ' https://cloud.google.com/cloud-build/docs/speeding-up-builds#using_a_cached_docker_image'
    ),
)

USE_KANIKO = flags.DEFINE_boolean(
    'xm_use_kaniko',
    False,
    'Use kaniko backend for Cloud Build and enable caching.',
)

KANIKO_CACHE_TTL = flags.DEFINE_string(
    'xm_kaniko_cache_ttl', '336h', 'Cache ttl to use for kaniko builds.'
)

GCP_SERVICE_ACCOUNT_NAME = flags.DEFINE_string(
    'xm_gcp_service_account_name',
    'xmanager',
    (
        'Specifies the user-managed service account name to be used by XManager'
        'Note that user-managed service accounts have the following format: '
        '`{service-account-name}@{project-id}.iam.gserviceaccount.com`, so only'
        'the part before @ is required'
    ),
)

K8S_SERVICE_ACCOUNT_NAME = flags.DEFINE_string(
    'xm_k8s_service_account_name',
    'default',
    (
        'Specifies the Kubernetes Service Account name to be used by XManager'
        ' inthe pod specifications.'
    ),
)

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
