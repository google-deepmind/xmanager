#!/bin/bash

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

readonly BAZEL_DIR="/tmp/bazel"
# copybara:strip_begin(xmc)
readonly GOOGLEAPIS_DIR="/tmp/googleapis"
# copybara:strip_end
readonly SOURCE_ROOT_DIR="$(realpath $(dirname $0))"
readonly GENERATED_DIR="$SOURCE_ROOT_DIR/generated"

if [ ! -d "${BAZEL_DIR}" ]; then
  git clone --depth 1 https://github.com/bazelbuild/bazel.git "${BAZEL_DIR}"
fi

# copybara:strip_begin(xmc)
if [ ! -d "${GOOGLEAPIS_DIR}" ]; then
  git clone --depth 1 https://github.com/googleapis/googleapis.git "${GOOGLEAPIS_DIR}"
fi
# copybara:strip_end

protoc --proto_path="${BAZEL_DIR}" --python_out="${BAZEL_DIR}" \
  "${BAZEL_DIR}/src/main/java/com/google/devtools/build/lib/buildeventstream/proto/build_event_stream.proto" \
  "${BAZEL_DIR}/src/main/protobuf/command_line.proto" \
  "${BAZEL_DIR}/src/main/protobuf/failure_details.proto" \
  "${BAZEL_DIR}/src/main/protobuf/invocation_policy.proto" \
  "${BAZEL_DIR}/src/main/protobuf/option_filters.proto"

cp "${BAZEL_DIR}/src/main/java/com/google/devtools/build/lib/buildeventstream/proto/build_event_stream_pb2.py" "${GENERATED_DIR}/"
cp "${BAZEL_DIR}/src/main/protobuf/command_line_pb2.py" "${GENERATED_DIR}/"
cp "${BAZEL_DIR}/src/main/protobuf/failure_details_pb2.py" "${GENERATED_DIR}/"
cp "${BAZEL_DIR}/src/main/protobuf/invocation_policy_pb2.py" "${GENERATED_DIR}/"
cp "${BAZEL_DIR}/src/main/protobuf/option_filters_pb2.py" "${GENERATED_DIR}/"

protoc --proto_path="$SOURCE_ROOT_DIR" --python_out="$SOURCE_ROOT_DIR" \
  "$SOURCE_ROOT_DIR/xm_local/storage/data.proto"
mv "$SOURCE_ROOT_DIR/xm_local/storage/data_pb2.py" "${GENERATED_DIR}/"

# Make generated imports local to xmanager.generated.
find "${GENERATED_DIR}/" -name '*pb2*' -exec \
  sed -i 's/^from src\.main.* import/from . import/' {} \;
# Add NOLINT line.
find "${GENERATED_DIR}/" -name '*pb2*' -exec \
  sed -i 's/DO NOT EDIT!/DO NOT EDIT!\n# pylint: skip-file/' {} \;

# copybara:strip_begin(xmc)
# Compile xmanager_cloud protos
if python3 -c "import grpc_tools" &> /dev/null; then
  echo "Compiling xmanager_cloud protos..."
  python3 -m grpc_tools.protoc \
    --proto_path="$SOURCE_ROOT_DIR" \
    --proto_path="${GOOGLEAPIS_DIR}" \
    --python_out="$SOURCE_ROOT_DIR" \
    --grpc_python_out="$SOURCE_ROOT_DIR" \
    "$SOURCE_ROOT_DIR"/xmanager_cloud/xid_service/proto/*.proto \
    "$SOURCE_ROOT_DIR"/xmanager_cloud/experiment_state_server/proto/*.proto
else
  echo "WARNING: grpc_tools not found. Skipping xmanager_cloud proto compilation."
  echo "Please install grpcio-tools: pip install grpcio-tools"
fi
# copybara:strip_end
