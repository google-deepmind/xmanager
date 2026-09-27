"""An HTTP server incrementing a value in Redis."""

from typing import Sequence

from absl import app
from absl import flags
import bottle
import redis

redis_host = flags.DEFINE_string("redis_host", None, "Address to Redis server.")

server = bottle.Bottle()
rdb = None


@server.route("/increment")
def increment():
  global rdb
  if rdb is None:
    return "Redis host not set."
  counter = rdb.incr("counter")
  return f"{counter=}"


@server.route("/")
def index():
  global rdb
  if rdb is None:
    return "Redis host not set."
  counter = int(rdb.get("counter") or 0)
  return f"{counter=}\nIncrement it by visiting `/increment`."


def main(argv: Sequence[str]) -> None:
  del argv

  global rdb
  rdb = redis.Redis(host=redis_host.value, decode_responses=True)

  max_attempts = 30
  for attempt in range(1, max_attempts + 1):
    print(f"Waiting for Redis to be available ({attempt}/{max_attempts})...")
    try:
      rdb.ping()
      break
    except redis.exceptions.ConnectionError:
      time.sleep(1)
  else:
    raise RuntimeError(
        f"Could not connect to Redis at {redis_host.value!r} after "
        f"{max_attempts} attempts."
    )

  # Initialize the counter only if it doesn't already exist so that the value
  # persists across server restarts via Redis' mounted data volume.
  rdb.setnx("counter", 0)

  bottle.run(server, host="0.0.0.0", port=8080, debug=False)


if __name__ == "__main__":
  app.run(main)
