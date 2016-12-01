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

from resource_management import *

# server configurations
config = Script.get_config()

hadoop_conf = config['configurations']['global']['hadoop.conf']
yarn_cg_root = config['configurations']['global']['yarn.cgroup.root']

user_name = config['configurations']['global']['user.name']
registry_zk = config['configurations']['global']['zookeeper.quorum']

user_scripts_entry = config['configurations']['global']['user.scripts.entry']
user_checkpoint_prefix = config['configurations']['global']['user.checkpoint.prefix']

docker_image = config['configurations']['global']['docker.image']

app_root = config['configurations']['global']['app_root']
app_log_dir = config['configurations']['global']['app_log_dir']
pid_file = config['configurations']['global']['pid_file']

container_id = config['configurations']['global']['app_container_id']

ps_port = config['configurations']['global']['ps.port']
chiefworker_port = config['configurations']['global']['chiefworker.port']
worker_port = config['configurations']['global']['worker.port']
tensorboard_port = config['configurations']['global']['tensorboard.port']
ports_dict = {"port.ps": ps_port,
              "port.chiefworker": chiefworker_port,
              "port.worker": worker_port,
              "port.tensorboard": tensorboard_port}

componentName = config['componentName']
service_name = config['serviceName']
hostname = config['hostname']
