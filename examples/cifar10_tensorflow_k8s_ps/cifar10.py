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

FLAGS = flags.FLAGS
flags.DEFINE_integer('epochs', 5, 'epochs')
flags.DEFINE_float('learning_rate', 0.001, 'learning rate')


def main(_):
  cluster_resolver = tf.distribute.cluster_resolver.TFConfigClusterResolver()

  if cluster_resolver.task_type in ('worker', 'ps'):
    os.environ['GRPC_FAIL_FAST'] = 'use_caller'

    server = tf.distribute.Server(
        cluster_resolver.cluster_spec(),
        job_name=cluster_resolver.task_type,
        task_index=cluster_resolver.task_id,
        protocol=cluster_resolver.rpc_layer or 'grpc',
        start=True,
    )
    server.join()

  (train_images, train_labels), _ = datasets.cifar10.load_data()

  def dataset_fn(input_context):
    dataset = tf.data.Dataset.from_tensor_slices(
        (train_images, train_labels)
    ).repeat()
    dataset = dataset.shard(
        input_context.num_input_pipelines, input_context.input_pipeline_id
    )
    dataset = dataset.batch(64)
    dataset = dataset.prefetch(2)

    return dataset

  strategy = tf.distribute.experimental.ParameterServerStrategy(
      cluster_resolver
  )
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

  model.fit(
      tf.keras.utils.experimental.DatasetCreator(dataset_fn),
      steps_per_epoch=1500,
      epochs=FLAGS.epochs,
  )


if __name__ == '__main__':
  app.run(main)
