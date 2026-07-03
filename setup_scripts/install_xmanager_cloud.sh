#!/bin/bash
# Copyright 2026 Google LLC
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

# --- Helper Functions ---

# Print error and exit
error_exit() {
  echo "Error: ${1}" >&2
  exit 1
}

# Get absolute path using python
get_absolute_path() {
  python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "${1}"
}

# Clean, recreate, and copy python files and protos to a specific XManager service package
copy_service_to_package() {
  local proto_subdir="${1}"
  local service_name="${2}"
  local dest_dir="${XMANAGER_SRC_DIR}/xmanager_cloud/${service_name}"

  # Recreate service directory
  mkdir -p "${dest_dir}"
  touch "${dest_dir}/__init__.py"

  # Copy Python files from XMC cmd directory (if they exist)
  local cmd_dir="${XMC_DIR}/cmd/${service_name}"
  if [ -d "${cmd_dir}" ]; then
    echo "Copying python files from ${cmd_dir}..."
    cp "${cmd_dir}"/*.py "${dest_dir}/" || echo "Warning: No python files found in ${cmd_dir}"
  fi

  # Clean and recreate proto destination
  local dest_proto_dir="${dest_dir}/proto"
  rm -rf "${dest_proto_dir}"
  mkdir -p "${dest_proto_dir}"
  touch "${dest_proto_dir}/__init__.py"

  # Copy proto contents
  cp -r "${XMC_PROTOS_SRC_DIR}/${proto_subdir}/." "${dest_proto_dir}/"
}

# Load dotenv file and export its variables if they are not already set in the environment
load_dotenv() {
  local dotenv_file="${1}"
  if [ -f "${dotenv_file}" ]; then
    echo "Loading environment variables from ${dotenv_file}..."
    while IFS= read -r line || [ -n "$line" ]; do
      # Remove leading/trailing whitespace
      line=$(echo "$line" | xargs)
      # Skip empty lines and comments (starting with #)
      if [[ -n "$line" && ! "$line" =~ ^# ]]; then
        # Strip leading "export " if present
        if [[ "$line" =~ ^export[[:space:]] ]]; then
          line="${line#export}"
          line=$(echo "$line" | xargs)
        fi
        local var_name="${line%%=*}"
        local var_value="${line#*=}"
        if [ -z "${!var_name}" ]; then
          echo "  Exporting ${var_name}=${var_value}"
          export "${var_name}=${var_value}"
        else
          echo "  ${var_name} is already set, skipping."
        fi
      fi
    done < "${dotenv_file}"
  fi
}

# --- Default Paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_XMANAGER_DIR="$(get_absolute_path "${SCRIPT_DIR}/..")"
DEFAULT_XMC_DIR="$(get_absolute_path "${SCRIPT_DIR}/../../xmc")"
DEFAULT_VENV_DIR="${SCRIPT_DIR}/venv"

# --- Parse Arguments ---
XMANAGER_DIR="${DEFAULT_XMANAGER_DIR}"
XMC_DIR="${DEFAULT_XMC_DIR}"
VENV_DIR="${DEFAULT_VENV_DIR}"
DOTENV_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --xmanager-dir)
      XMANAGER_DIR="$2"
      shift 2
      ;;
    --xmc-dir)
      XMC_DIR="$2"
      shift 2
      ;;
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    --dotenv)
      DOTENV_PATH="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --xmanager-dir <path>  Path to local xmanager repo containing setup.py (default: ${DEFAULT_XMANAGER_DIR})"
      echo "  --xmc-dir <path>       Path to local xmc repo (default: ${DEFAULT_XMC_DIR})"
      echo "  --venv <path>          Path to target virtual environment (default: ${DEFAULT_VENV_DIR})"
      echo "  --dotenv <path>        Path to dotenv file (default: not set)"
      exit 0
      ;;
    *)
      error_exit "Unknown option: $1"
      ;;
  esac
done

# Resolve paths to absolute
XMANAGER_DIR="$(get_absolute_path "${XMANAGER_DIR}")"
XMC_DIR="$(get_absolute_path "${XMC_DIR}")"
VENV_DIR="$(get_absolute_path "${VENV_DIR}")"
if [ -n "${DOTENV_PATH}" ]; then
  DOTENV_PATH="$(get_absolute_path "${DOTENV_PATH}")"
fi

# Validation
if [ ! -d "${XMANAGER_DIR}" ]; then
  error_exit "XManager directory not found at ${XMANAGER_DIR}"
fi
if [ ! -d "${XMC_DIR}" ]; then
  error_exit "XMC directory not found at ${XMC_DIR}"
fi
if [ ! -d "${XMC_DIR}/api/protos" ]; then
  error_exit "XMC protos directory not found at ${XMC_DIR}/api/protos"
fi
if [ -n "${DOTENV_PATH}" ] && [ ! -f "${DOTENV_PATH}" ]; then
  error_exit "Dotenv file not found at ${DOTENV_PATH}"
fi

# Detect xm_cloud relative path (different in workspace vs exported)
if [ -d "${XMANAGER_DIR}/xmanager/xm_cloud" ]; then
  XM_CLOUD_REL_PATH="xmanager/xm_cloud"
else
  XM_CLOUD_REL_PATH="xm_cloud"
fi

echo "=== Configuration ==="
echo "XManager Source: ${XMANAGER_DIR}"
echo "XMC Source:      ${XMC_DIR}"
echo "Venv Target:     ${VENV_DIR}"
if [ -n "${DOTENV_PATH}" ]; then
  echo "Dotenv File:     ${DOTENV_PATH}"
else
  echo "Dotenv File:     [None]"
fi
echo "====================="

# Load environment variables from dotenv file
if [ -n "${DOTENV_PATH}" ]; then
  load_dotenv "${DOTENV_PATH}"
fi

# Create temporary directory for setup
TEMP_DIR="$(mktemp -d)"

# Clean up temporary clones and builds on completion or failure
trap 'echo "Cleaning up temporary setup files..."; rm -rf "${TEMP_DIR}"' EXIT

XMANAGER_SRC_DIR="${TEMP_DIR}/xmanager"
GOOGLEAPIS_SRC_DIR="${TEMP_DIR}/googleapis"
XMC_PROTOS_SRC_DIR="${XMC_DIR}/api/protos"

# 1. Setup Virtual Environment
if [ ! -d "${VENV_DIR}" ]; then
  echo "Creating virtual environment at ${VENV_DIR}..."
  python3 -m venv "${VENV_DIR}"
fi
source "${VENV_DIR}/bin/activate"

echo "Updating pip and installing dependencies..."
pip install --index-url https://pypi.org/simple --upgrade pip
pip install --index-url https://pypi.org/simple \
  grpcio \
  grpcio-tools \
  google-auth \
  googleapis-common-protos \
  requests \
  python-dotenv \
  GitPython \
  psutil \
  tabulate \
  absl-py

# 2. Copy local XManager code (excluding venvs, git, and build artifacts)
echo "Copying local XManager code..."
if command -v rsync >/dev/null 2>&1; then
  rsync -a \
    --exclude=.git \
    --exclude=.venv \
    --exclude=venv \
    --exclude=__pycache__ \
    --exclude=build \
    --exclude=dist \
    --exclude=*.egg-info \
    "${XMANAGER_DIR}/" "${XMANAGER_SRC_DIR}/"
else
  echo "Warning: rsync not found. Falling back to cp."
  mkdir -p "${XMANAGER_SRC_DIR}"
  # cp -r "${XMANAGER_DIR}"/* "${XMANAGER_SRC_DIR}/"
  # rm -rf "${XMANAGER_SRC_DIR}/.git" "${XMANAGER_SRC_DIR}/.venv" "${XMANAGER_SRC_DIR}/venv"
  cp -r "${XMANAGER_DIR}/." "${XMANAGER_SRC_DIR}/"
  # Manually remove the junk
  rm -rf "${XMANAGER_SRC_DIR}/.git" "${XMANAGER_SRC_DIR}/venv" "${XMANAGER_SRC_DIR}"/**/__pycache__
fi

# Copy unit tests
echo "Copying artifact unit tests..."
cp "${XMANAGER_DIR}/${XM_CLOUD_REL_PATH}/artifact_test.py" "${XMANAGER_SRC_DIR}/${XM_CLOUD_REL_PATH}/artifact_test.py"

# Create package namespace file for xmanager_cloud
mkdir -p "${XMANAGER_SRC_DIR}/xmanager_cloud"
touch "${XMANAGER_SRC_DIR}/xmanager_cloud/__init__.py"

# 3. Clone Google APIs repository (required for proto compilation)
echo "Cloning Google APIs repository..."
git clone --depth 1 https://github.com/googleapis/googleapis.git "${GOOGLEAPIS_SRC_DIR}"

# 4. Copy Protos and Python wrapper into XManager source
echo "Copying xmc protos and source files..."
copy_service_to_package "xid" "xid_service"
copy_service_to_package "experiment" "experiment_state_server"

# 5. Adjust proto imports (rewrite third_party/xmanager_cloud/ to xmanager_cloud/)
echo "Adjusting proto and python import paths..."
if sed --version >/dev/null 2>&1; then
  SED_I=(sed -i)
else
  SED_I=(sed -i '')
fi
find "${XMANAGER_SRC_DIR}/xmanager_cloud/xid_service/proto" \
  "${XMANAGER_SRC_DIR}/xmanager_cloud/experiment_state_server/proto" \
  -name "*.proto" -type f -exec "${SED_I[@]}" 's|third_party/xmanager_cloud/|xmanager_cloud/|g' {} +

# Adjust python imports in copied files
find "${XMANAGER_SRC_DIR}/xmanager_cloud/experiment_state_server" \
  -name "*.py" -type f -exec "${SED_I[@]}" 's|from longrunning|from google.longrunning|g' {} +

# Adjust python imports in copied test files
"${SED_I[@]}" 's|g'oogle3'.third_party.||g' "${XMANAGER_SRC_DIR}/${XM_CLOUD_REL_PATH}/artifact_test.py"

# 6. Compile protos
echo "Compiling xmanager_cloud protos..."
python3 -m grpc_tools.protoc \
  --proto_path="${XMANAGER_SRC_DIR}" \
  --proto_path="${GOOGLEAPIS_SRC_DIR}" \
  --python_out="${XMANAGER_SRC_DIR}" \
  --grpc_python_out="${XMANAGER_SRC_DIR}" \
  "${XMANAGER_SRC_DIR}"/xmanager_cloud/xid_service/proto/*.proto \
  "${XMANAGER_SRC_DIR}"/xmanager_cloud/experiment_state_server/proto/*.proto

# 7. Install package
echo "Installing XManager library..."
# This builds and installs the packages directly to site-packages
pip install --index-url https://pypi.org/simple "${XMANAGER_SRC_DIR}"

# 8. Verify Installation
echo "Verifying imports..."
(
  cd /tmp
  python3 -c "from xmanager import xm_cloud; print('Successfully imported xm_cloud:', xm_cloud)"
  python3 -c "import xmanager_cloud; print('Successfully imported xmanager_cloud:', xmanager_cloud)"
  python3 -c "import xmanager_cloud.experiment_state_server.experiment_state_api; print('Successfully imported experiment_state_api')"
  python3 -c "import xmanager_cloud.experiment_state_server.proto.api_pb2; print('Successfully imported api_pb2')"
)

# 9. Run unit tests
echo "Running experiment unit tests inside venv..."
(
  cd /tmp
  python3 -m unittest discover -s "${XMANAGER_SRC_DIR}/${XM_CLOUD_REL_PATH}" -p "*_test.py"
)

echo "=================================================="
echo "Installation complete!"
echo "To activate the virtual environment, run:"
echo "  source ${VENV_DIR}/bin/activate"
echo "=================================================="