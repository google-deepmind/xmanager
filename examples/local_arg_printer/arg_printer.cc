// Copyright 2021 DeepMind Technologies Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A simple C++ binary that prints its command-line arguments to a file.
//
// The output file path is read from the `OUTPUT_PATH` environment variable.
// The first line of the output file will contain the argument count, followed
// by each argument on a new line. The program then sleeps for 5 seconds to
// simulate a long-running job before exiting.
// Usage:
// ```
// $ OUTPUT_PATH="/tmp/output.txt" bazel run //:arg_printer -- "arg1" "arg2"
// $ cat /tmp/output.txt
// 3
// /home/user/.cache/bazel/_bazel_user/b2b20ce9d34fd5389bf343c4cc37f48f/execroot/_main/bazel-out/k8-fastbuild/bin/arg_printer
// arg1
// arg2
// ```

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

int main(int argc, char* argv[]) {
  std::ofstream ouf(std::getenv("OUTPUT_PATH"));
  ouf << argc << std::endl;
  for (int i = 0; i < argc; ++i) {
    ouf << argv[i] << std::endl;
  }
  // Sleep to demonstrate local jobs waiting.
  std::this_thread::sleep_for(std::chrono::seconds(5));
  return 0;
}
