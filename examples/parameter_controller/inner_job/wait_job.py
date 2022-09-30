"""Job waiting."""
import time

from absl import app
from absl import flags

_TIME_TO_SLEEP = flags.DEFINE_integer('time_to_sleep', 10, 'Time to sleep.')


def main(_):
  print(f'Hello, waiting for {_TIME_TO_SLEEP.value}...')
  time.sleep(_TIME_TO_SLEEP.value)
  print('Done!')


if __name__ == '__main__':
  app.run(main)
