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
"""A launcher for server.py and Redis.

See README.md for details.
"""

from typing import Sequence

from absl import app
from xmanager import xm
from xmanager import xm_local


def main(argv: Sequence[str]) -> None:
  del argv  # Unused.

  with xm_local.create_experiment(
      experiment_title='local_container_links') as experiment:
    [redis, server] = experiment.package([
        xm.Packageable(
            executable_spec=xm.Container(image_path='redis'),
            executor_spec=xm_local.Local.Spec(),
        ),
        xm.Packageable(
            executable_spec=xm.BazelContainer(label='//:server_image.tar'),
            executor_spec=xm_local.Local.Spec(),
        ),
    ])

    async def generator(work_unit):
      work_unit.add(
          xm.JobGroup(
              server=xm.Job(
                  executable=server,
                  executor=xm_local.Local(
                      docker_options=xm_local.DockerOptions(
                          ports={8080: 8080})),
                  args={'redis_host': work_unit.get_full_job_name('redis')},
              ),
              redis=xm.Job(
                  name='redis',
                  executable=redis,
                  executor=xm_local.Local(
                      docker_options=xm_local.DockerOptions(
                          volumes={'/tmp/redis': '/data'})),
              ),
          ))

    experiment.add(generator)


if __name__ == '__main__':
  app.run(main)
