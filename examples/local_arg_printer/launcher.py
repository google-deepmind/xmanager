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
"""XManager launcher that runs locally a binary built with Bazel.

One must `cd` into xmanager/examples/local_arg_printer/ in order to run this
example because Bazel needs to locate the WORKSPACE file.
"""

from typing import Sequence

from absl import app
from xmanager import xm
from xmanager import xm_local


def main(argv: Sequence[str]) -> None:
  del argv

  with xm_local.create_experiment(
      experiment_title='local_arg_printer') as experiment:
    [executable] = experiment.package([
        xm.Packageable(
            executable_spec=xm.BazelBinary(
                label='//:arg_printer'
            ),
            executor_spec=xm_local.Local.Spec(),
        ),
    ])
    experiment.add(
        xm.Job(
            executable=executable,
            executor=xm_local.Local(),
            env_vars={'OUTPUT_PATH': '/tmp/local_arg_printer.txt'},
        ))


if __name__ == '__main__':
  app.run(main)
