"""An HTTP server incrementing a value in Redis."""

import time
from typing import Sequence

from absl import app
from absl import flags
from flask import Flask
import redis

redis_host = flags.DEFINE_string("redis_host", None, "Address to Redis server.")

server = Flask(__name__)


@server.route("/increment")
def increment():
  rdb = server.config["RDB"]
  counter = rdb.incr("counter")
  return f"{counter=}"


@server.route("/")
def index():
  rdb = server.config["RDB"]
  counter = int(rdb.get("counter"))
  return f"{counter=}\nIncrement it by visiting `/increment`."


def main(argv: Sequence[str]) -> None:
  del argv

  rdb = redis.Redis(host=redis_host.value, decode_responses=True)

  while True:
    print("Waiting for Redis to be available...")
    try:
      rdb.ping()
      break
    except redis.exceptions.ConnectionError:
      time.sleep(1)

  server.config["RDB"] = rdb
  rdb.set("counter", 0)

  server.run(host="0.0.0.0", port=8080, debug=False)


if __name__ == "__main__":
  app.run(main)
