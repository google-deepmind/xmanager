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
#
# Builds the XManager PyPI sdist and wheel packages, including fetching and compiling
# XManager Cloud (XMC) protos and Python clients.

set -e

# Determine the workspace root directory (assuming script is run from or located at setup_scripts/build_pypi.sh)
SOURCE_ROOT_DIR="$(realpath "$(dirname "$0")/..")"
cd "${SOURCE_ROOT_DIR}"

# Specify the Python version to use for the release build.
PYTHON_CMD="${PYTHON_CMD:-python3.11}"

VENV_DIR="/tmp/xm_build_venv"
echo "Creating and activating virtual environment at ${VENV_DIR}..."
$PYTHON_CMD -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

echo "Installing packaging and proto compilation dependencies..."
pip install --upgrade pip build twine grpcio grpcio-tools google-auth googleapis-common-protos requests

# Clone xmc repository to include proto files and python client logic.
# By default, this fetches the latest tag, or you can specify a tag (e.g. RELEASE_TAG=0.1.0).
RELEASE_TAG=${RELEASE_TAG:-$(git ls-remote --tags --sort=-v:refname https://github.com/google/xmc.git | grep -o 'refs/tags/[^^{}]*' | head -n 1 | sed 's#refs/tags/##')}
echo "Cloning xmc tag: ${RELEASE_TAG}"
git clone --depth 1 --branch "${RELEASE_TAG}" https://github.com/google/xmc.git xmc_repo

echo "Creating xmanager_cloud package structure..."
mkdir -p xmanager_cloud/experiment_state_server/proto
mkdir -p xmanager_cloud/xid_service/proto

echo "Copying Python client wrapper..."
cp xmc_repo/cmd/experiment_state_server/experiment_state_api.py xmanager_cloud/experiment_state_server/

echo "Copying proto files from xmc api folder..."
cp xmc_repo/api/protos/experiment/*.proto xmanager_cloud/experiment_state_server/proto/
cp xmc_repo/api/protos/xid/*.proto xmanager_cloud/xid_service/proto/

GOOGLEAPIS_DIR="/tmp/googleapis"
if [ ! -d "${GOOGLEAPIS_DIR}" ]; then
  echo "Cloning googleapis repository..."
  git clone --depth 1 https://github.com/googleapis/googleapis.git "${GOOGLEAPIS_DIR}"
fi

# To ensure protoc succeeds regardless of whether internal proto import paths (third_party/xmanager_cloud/...)
# or external paths (api/protos/...) are used in the .proto files, create symlinks for both structures.
mkdir -p third_party
ln -sfn ../xmanager_cloud third_party/xmanager_cloud

echo "Compiling xmanager_cloud protos..."
python3 -m grpc_tools.protoc \
  --proto_path=. \
  --proto_path=xmc_repo \
  --proto_path="${GOOGLEAPIS_DIR}" \
  --python_out=. \
  --grpc_python_out=. \
  ./xmanager_cloud/xid_service/proto/*.proto \
  ./xmanager_cloud/experiment_state_server/proto/*.proto

echo "Verifying imports..."
python3 -c "import xmanager_cloud"
python3 -c "import xmanager_cloud.experiment_state_server.experiment_state_api"
python3 -c "import xmanager_cloud.experiment_state_server.proto.api_pb2"

echo "Cleaning up temporary build directories..."
rm -rf xmc_repo third_party

echo "Building sdist and wheel packages..."
python3 -m build

echo "Cleaning up virtual environment..."
rm -rf "${VENV_DIR}"

echo "Build completed successfully! Packages are available in dist/"
