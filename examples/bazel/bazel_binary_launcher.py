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
"""XManager launcher example building with Bazel.

You must `cd` into xmanager/examples/bazel/ in order to run this example
because you need to be in the same directory as WORKSPACE.
"""

from typing import Sequence

from absl import app
from xmanager import xm
from xmanager import xm_local


def main(argv: Sequence[str]) -> None:
  del argv
  with xm_local.create_experiment(experiment_name='bazel_binary') as experiment:
    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=xm.BazelBinary(
                label='//:binary'
            ),
            executor_spec=xm_local.Local.Spec(),
        ),
    ])
    print(repr(executable))
    experiment.add(xm.Job(executable=executable, executor=xm_local.Local()))


if __name__ == '__main__':
  app.run(main)
