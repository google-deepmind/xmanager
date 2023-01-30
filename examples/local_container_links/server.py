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
"""An HTTP server incrementing a value in Redis."""

from typing import Sequence

from absl import app
from absl import flags
import bottle
import bottle.ext.redis

redis_host = flags.DEFINE_string('redis_host', None, "Redis' host.")

server = bottle.Bottle()


@server.route('/increment')
def increment(rdb):
  return str(rdb.incr('counter'))


def main(argv: Sequence[str]) -> None:
  del argv  # Unused.

  server.install(bottle.ext.redis.RedisPlugin(host=redis_host.value))
  bottle.run(server, host='0.0.0.0', port=8080, debug=True)


if __name__ == '__main__':
  app.run(main)
