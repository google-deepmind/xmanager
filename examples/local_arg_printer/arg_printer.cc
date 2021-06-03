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
