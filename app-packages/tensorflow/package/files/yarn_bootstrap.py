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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
from abc import abstractmethod, ABCMeta
import tensorflow as tf

flags = tf.app.flags
# Flags for configuring the task
flags.DEFINE_string("job_name", None, "job name: worker or ps")
flags.DEFINE_integer("task_index", 0, "Worker task index, should be >= 0")
flags.DEFINE_string("ps_hosts", "", "Comma-separated list of hostname:port pairs")
flags.DEFINE_string("worker_hosts", "", "Comma-separated list of hostname:port pairs")
flags.DEFINE_string("ckp_dir", None, "Directory for storing the checkpoints")
flags.DEFINE_string("work_dir", "/tmp/tf_on_yarn", "Work directory")

FLAGS = flags.FLAGS

class YarnBootstrap(object):
  def __init__(self):
    pass

  __metaclass__ = ABCMeta

  @abstractmethod
  def worker_do(self, server, cluster_spec, task_id):
    pass

  @abstractmethod
  def ps_do(self, server, cluster_spec, task_id):
    pass

  def device_and_server(self):
    # If FLAGS.job_name is not set, we're running single-machine TensorFlow.
    # Don't set a device.
    if FLAGS.job_name is None:
      print("Running single-machine training")
      return (None, "", "")

    # Otherwise we're running distributed TensorFlow.
    print("Running distributed training")
    if FLAGS.task_index is None or FLAGS.task_index == "":
      raise ValueError("Must specify an explicit `task_index`")
    if FLAGS.ps_hosts is None or FLAGS.ps_hosts == "":
      raise ValueError("Must specify an explicit `ps_hosts`")
    if FLAGS.worker_hosts is None or FLAGS.worker_hosts == "":
      raise ValueError("Must specify an explicit `worker_hosts`")

    cluster_spec = tf.train.ClusterSpec({
      "ps": FLAGS.ps_hosts.split(","),
      "worker": FLAGS.worker_hosts.split(","),
    })
    server = tf.train.Server(
      cluster_spec, job_name=FLAGS.job_name, task_index=FLAGS.task_index)
    time.sleep(60)
    if FLAGS.job_name == "ps":
      self.ps_do(server, cluster_spec, FLAGS.task_index)
      server.join()

    worker_device = "/job:worker/task:{}".format(FLAGS.task_index)
    return (
      tf.train.replica_device_setter(
        worker_device=worker_device,
        cluster=cluster_spec),
      server, cluster_spec
    )

  def start(self, unused_args):
    if FLAGS.ckp_dir is None or FLAGS.ckp_dir == "":
      raise ValueError("Must specify an explicit `ckp_dir`")
    device, server, cluster_spec = self.device_and_server()
    with tf.device(device):
      self.worker_do(server, cluster_spec, FLAGS.task_index)

