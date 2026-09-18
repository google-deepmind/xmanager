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
PYTHON_CMD="${PYTHON_CMD:-python3}"

VENV_DIR="/tmp/xm_build_venv"

cleanup() {
  echo "Cleaning up temporary build artifacts..."
  rm -rf "${SOURCE_ROOT_DIR}/xmc_repo" \
         "${SOURCE_ROOT_DIR}/third_party" \
         "${SOURCE_ROOT_DIR}/xmanager_cloud" \
         "${SOURCE_ROOT_DIR}/xdash" \
         "${SOURCE_ROOT_DIR}/build" \
         "${SOURCE_ROOT_DIR}/xmanager.egg-info" \
         "${VENV_DIR}"
}
trap cleanup EXIT

echo "Creating and activating virtual environment at ${VENV_DIR}..."
rm -rf "${VENV_DIR}"
$PYTHON_CMD -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

echo "Installing packaging and proto compilation dependencies..."
pip install --upgrade pip build twine grpcio "grpcio-tools" "protobuf<7" google-auth googleapis-common-protos requests

# Clone xmc repository to include proto files and python client logic.
# By default, this fetches the latest tag, or you can specify a tag (e.g. RELEASE_TAG=0.1.0).
RELEASE_TAG=${RELEASE_TAG:-$(git ls-remote --tags --sort=-v:refname https://github.com/google/xmc.git | grep -o 'refs/tags/[^^{}]*' | head -n 1 | sed 's#refs/tags/##')}
echo "Cloning xmc tag: ${RELEASE_TAG}"
rm -rf xmc_repo
git clone --depth 1 --branch "${RELEASE_TAG}" https://github.com/google/xmc.git xmc_repo

echo "Preparing and compiling xmanager_cloud protos via install_xmanager_cloud.sh..."
bash ./setup_scripts/install_xmanager_cloud.sh \
  --xmanager-dir "${SOURCE_ROOT_DIR}" \
  --xmc-dir "${SOURCE_ROOT_DIR}/xmc_repo" \
  --venv "${VENV_DIR}" \
  --prepare-only

echo "Cleaning up temporary clone directories before build..."
rm -rf xmc_repo third_party

echo "Building sdist and wheel packages..."
python3 -m build

echo "Build completed successfully! Packages are available in dist/"
