#!/bin/sh
#
# Installs XManager.

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

sudo apt-get update
sudo apt-get install -y git

python3 -m pip install --user --upgrade setuptools
python3 -m pip install --user git+https://github.com/deepmind/xmanager

if [ -n "${BASH_VERSION}" ]; then
  echo "Adding ~/.local/bin to PATH in ~/.bashrc..."
  # TODO: Use sed to search and replace instead of simply appending
  echo "export PATH=${PATH}:~/.local/bin" >> ~/.bashrc
  export PATH="${PATH}:~/.local/bin"
else
  echo "Add ~/.local/bin/xmanager to your PATH to directly launch xmanager from the shell."
fi