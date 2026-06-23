#!/bin/bash
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

# Determine the workspace root directory.
SOURCE_ROOT_DIR="$(realpath "$(dirname "$0")")"

if [ ! -d "$SOURCE_ROOT_DIR/venv" ]; then
  echo "Virtual environment 'venv' not found. Creating it..."
  python3 -m venv "$SOURCE_ROOT_DIR/venv"
fi

echo "Activating virtual environment..."
# shellcheck disable=SC1091
source "$SOURCE_ROOT_DIR/venv/bin/activate"

echo "Installing required python dependencies..."
# We use pypi.org directly to bypass potential local corp staging registry index-url constraints.
pip install --index-url https://pypi.org/simple \
  grpcio \
  grpcio-tools \
  google-auth \
  googleapis-common-protos \
  requests

# Clone external protos (googleapis)
GOOGLEAPIS_DIR="/tmp/googleapis"
if [ ! -d "${GOOGLEAPIS_DIR}" ]; then
  echo "Cloning googleapis repository..."
  git clone --depth 1 https://github.com/googleapis/googleapis.git "${GOOGLEAPIS_DIR}"
fi

echo "Compiling xmanager_cloud protos..."
python3 -m grpc_tools.protoc \
  --proto_path="${SOURCE_ROOT_DIR}" \
  --proto_path="${GOOGLEAPIS_DIR}" \
  --python_out="${SOURCE_ROOT_DIR}" \
  --grpc_python_out="${SOURCE_ROOT_DIR}" \
  "${SOURCE_ROOT_DIR}"/xmanager_cloud/xid_service/proto/*.proto \
  "${SOURCE_ROOT_DIR}"/xmanager_cloud/experiment_state_server/proto/*.proto

echo "Verifying imports..."
python3 -c "import xmanager_cloud"
python3 -c "import xmanager_cloud.experiment_state_server.experiment_state_api"
python3 -c "import xmanager_cloud.experiment_state_server.proto.api_pb2"

echo "Setup completed successfully!"