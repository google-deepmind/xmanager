#!/bin/sh
#
# Creates a basic GCP configuration based on user preferences.

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

set -e

gcloud_sdk_config_path=~/.config/gcloud
if [ -n "${GCLOUD_SDK_CONFIG_DIR}" ]; then
  gcloud_sdk_config_path="${GCLOUD_SDK_CONFIG_DIR}"
fi

# Note: `read -p` is not POSIX compliant.
read_prompt() {
  printf "%s" "$1"
  read -r "$2"
}

get_confirmation() {
  read_prompt "$1 [Y/n] " response

  case "${response}" in
    # Matches "y", "yes" (case insensitive) or default value
    [yY][eE][sS]|[yY]|[""]) true;;
    *) false;;
  esac
}

select_project() {
  # TODO: Handle (unset) stderr output
  current_project_id="$(gcloud config get project)"

  if [ -z "${current_project_id}" ]; then
    if get_confirmation "No default GCP project is defined. Do you have a project you want to use?"; then
      read_prompt "Project ID: " project_id

      (set -x; gcloud config set project "${project_id}")
      current_project_id="${project_id}"
    else
      # TODO: Create a project via https://cloud.google.com/sdk/gcloud/reference/projects/create
      echo "Create a GCP project at https://console.cloud.google.com and try again."
      exit 1
    fi
  fi

  echo "[x] Using project with ID ${current_project_id}"
}

authenticate_account() {
  # TODO: Handle (unset) stderr output
  current_account=$(gcloud config get account)

  if [ -z "${current_account}" ]; then
    (set -x; gcloud auth login)
    current_account=$(gcloud config get account)
  fi

  echo "[x] Using account ${current_account}"
}

authenticate_service_account() {
  if [ ! -f "${gcloud_sdk_config_path}/application_default_credentials.json" ]; then
    (set -x; gcloud auth application-default login)
  fi

  echo "[x] Using application default credentials stored in ${gcloud_sdk_config_path}/application_default_credentials.json"
}

enable_required_api() {
  if ! echo "${enabled_apis}" | grep -Exq "$1"; then
    if get_confirmation "API $1 is not enabled. Would you like to enable it?"; then
      (set -x; gcloud services enable "$1")
    else
      echo "Enabling $1 is required for running XManager. Exiting..."
      exit 1
    fi
  else
    echo "API $1 is enabled..."
  fi
}

enable_required_gcp_apis() {
  enabled_apis=$(gcloud services list --enabled | awk 'NR > 1 {print $1}')

  enable_required_api "iam.googleapis.com"

  # TODO: Handle billing account association requirement
  enable_required_api "aiplatform.googleapis.com"
  enable_required_api "containerregistry.googleapis.com"

  echo "[x] Required APIs are enabled"
}

select_bucket() {
  if [ -z "${GOOGLE_CLOUD_BUCKET_NAME}" ]; then
    read_prompt "Name of your cloud storage bucket (if it doesn't already exist, a new one will be created): " bucket_name

    user_buckets="$(gsutil ls)"

    if ! echo "${user_buckets}" | grep -Exq "gs://${bucket_name}/"; then
      # TODO: Make location configurable by the user
      (set -x; gsutil mb -l us-central1 "gs://${bucket_name}")
    fi

    export GOOGLE_CLOUD_BUCKET_NAME="${bucket_name}"

    if [ -n "${BASH_VERSION}" ]; then
      echo "Exporting GOOGLE_CLOUD_BUCKET_NAME=${bucket_name} in ~/.bashrc..."
      # TODO: Use sed to search and replace instead of simply appending
      echo "export GOOGLE_CLOUD_BUCKET_NAME=${bucket_name}" >> ~/.bashrc
    else
      echo "You have to add export GOOGLE_CLOUD_BUCKET_NAME=${bucket_name} to the shell startup script to make the change persistent."
    fi
  fi

  echo "[x] Using cloud storage bucket with name ${GOOGLE_CLOUD_BUCKET_NAME}"
}

echo "=== PROJECT SELECTION ==="
echo
select_project
echo

echo "=== AUTHENTICATION ==="
echo
authenticate_account
authenticate_service_account
echo

echo "=== REQUIRED SERVICES ==="
echo
enable_required_gcp_apis
echo

echo "=== BUCKET SELECTION ==="
echo
select_bucket
echo

echo "=== CONFIGURE DOCKER WITH GOOGLE CLOUD ==="
echo
(set -x; gcloud auth configure-docker)
echo

echo
echo "GCP configuration finished successfully"