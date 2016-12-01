#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from __future__ import division
from __future__ import print_function

import os

import tensorflow as tf
from tensorflow.examples.tutorials.mnist import mnist
from tensorflow.python.training import training_util
from yarn_bootstrap import *

FLAGS = tf.app.flags.FLAGS

class Ymnist(YarnBootstrap):
  def worker_do(self, server, cluster_spec, task_id):
    print("Checkpoint dir: " + FLAGS.ckp_dir)
    images, labels = self.inputs(100)
    logits = mnist.inference(images, 128, 128)
    loss = mnist.loss(logits, labels)
    train_op = mnist.training(loss, 0.01)
    target = "" if server == "" else server.target
    with tf.train.MonitoredTrainingSession(
        master=target,
        is_chief=(task_id == 0),
        checkpoint_dir=FLAGS.ckp_dir) as sess:
        step = 0
        while not sess.should_stop() and step < 1000000:
          sess.run(train_op)
          step = training_util.global_step(sess, training_util.get_global_step(sess.graph))
          print("Global step " + str(step))

  def ps_do(self, server, cluster_spec, task_id):
    print("Starting ps " + str(task_id))

  def read_and_decode(self, filename_queue):
    reader = tf.TFRecordReader()
    _, serialized_example = reader.read(filename_queue)
    features = tf.parse_single_example(
      serialized_example,
      # Defaults are not specified since both keys are required.
      features={
        'image_raw': tf.FixedLenFeature([], tf.string),
        'label': tf.FixedLenFeature([], tf.int64),
      })
    image = tf.decode_raw(features['image_raw'], tf.uint8)
    image.set_shape([mnist.IMAGE_PIXELS])
    # Convert from [0, 255] -> [-0.5, 0.5] floats.
    image = tf.cast(image, tf.float32) * (1. / 255) - 0.5
    # Convert label from a scalar uint8 tensor to an int32 scalar.
    label = tf.cast(features['label'], tf.int32)
    return image, label

  def inputs(self, batch_size):
    filename = os.path.join("hdfs://hdpdev/user/danrtsey.wy/mnist-data", "train.tfrecords")
    with tf.name_scope('input'):
      filename_queue = tf.train.string_input_producer([filename])
      image, label = self.read_and_decode(filename_queue)
      images, sparse_labels = tf.train.shuffle_batch(
        [image, label], batch_size=batch_size, num_threads=2,
        capacity=1000 + 3 * batch_size,
        # Ensures a minimum amount of shuffling of examples.
        min_after_dequeue=1000)
      return images, sparse_labels

def main(unused_argv):
  Ymnist().start(unused_args=unused_argv)

if __name__ == "__main__":
  tf.app.run()
