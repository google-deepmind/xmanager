# Copyright 2021 The Tensorflow Authors.
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
"""Code based on https://www.tensorflow.org/tutorials/images/cnn."""
import os

from absl import app
from absl import flags
import tensorflow as tf
from tensorflow.keras import datasets
from tensorflow.keras import layers
from tensorflow.keras import models

# When using Vertex Tensorboard, the tensorboard will be present as a
# environment variable.
LOG_DIR = os.environ.get('AIP_TENSORBOARD_LOG_DIR', '')

FLAGS = flags.FLAGS
flags.DEFINE_integer('epochs', 5, 'epochs')
flags.DEFINE_float('learning_rate', 0.001, 'learning rate')


def main(_):
  (train_images, train_labels), (test_images, test_labels) = (
      datasets.cifar10.load_data()
  )

  # Normalize pixel values to be between 0 and 1
  train_images, test_images = train_images / 255.0, test_images / 255.0

  strategy = tf.distribute.MirroredStrategy()
  with strategy.scope():
    model = models.Sequential()
    model.add(
        layers.Conv2D(32, (3, 3), activation='relu', input_shape=(32, 32, 3))
    )
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))
    model.add(layers.MaxPooling2D((2, 2)))
    model.add(layers.Conv2D(64, (3, 3), activation='relu'))

    model.add(layers.Flatten())
    model.add(layers.Dense(64, activation='relu'))
    model.add(layers.Dense(10))

    optimizer = tf.keras.optimizers.Adam(learning_rate=FLAGS.learning_rate)
    model.compile(
        optimizer=optimizer,
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=['accuracy'],
    )

    callbacks = []
    if LOG_DIR:
      callbacks = [
          tf.keras.callbacks.TensorBoard(
              log_dir=LOG_DIR,
              histogram_freq=1,
          ),
      ]

  model.fit(
      train_images,
      train_labels,
      epochs=FLAGS.epochs,
      validation_data=(test_images, test_labels),
      callbacks=callbacks,
      verbose=2,
  )


if __name__ == '__main__':
  app.run(main)
