## Description

launcher.py packages and executes (locally) two Docker containers:
[Redis](https://hub.docker.com/_/redis), a key-value database, and a
[Bottle](https://bottlepy.org/)-based HTTP server that listens on port 8080 and
responds to requests at `/increment` by running
[`INCR counter`](https://redis.io/commands/INCR) on Redis and sending back the
result.

## Instructions

1. Clone the repository and install XManager via `pip install ./xmanager`.
2. Install Bazelisk via `npm install -g @bazel/bazelisk`.
3. Get into this directory and run `xmanager launch launcher.py -- --xm_bazel_command=bazelisk`.
4. Once it is idling, open `http://localhost:8080/increment` in the browser.
