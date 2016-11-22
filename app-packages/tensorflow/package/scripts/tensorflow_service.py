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

import sys
import os
import time
from resource_management import *


def tensorflow_service(action='start'):  # 'start' or 'stop' or 'status'
  import params
  import functions
  container_id = format("{container_id}")
  application_id = functions.get_application_id(container_id)
  componentName = format("{componentName}")

  if action == 'start':
    checkpoint_dir = format("{user_checkpoint_prefix}/{service_name}.{application_id}")
    # Get allocated resources
    mem_ps,vcore_ps,mem_worker,vcore_worker,mem_tb,vcore_tb = functions.get_allocated_resources_num()
    # Always launch role tensorboard
    if componentName == "tensorboard":
      daemon_cmd = format("/usr/bin/docker run -d -u $(id -u yarn) --cgroup-parent={yarn_cg_root}/{container_id} -m {mem_tb}m " \
                   "-v {hadoop_conf}:/usr/local/hadoop/etc/hadoop " \
                   "-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group " \
                   "-p {tensorboard_port}:{tensorboard_port} --name={container_id} {docker_image} " \
                   "/bin/bash -c 'tensorboard --logdir={checkpoint_dir} --port={tensorboard_port}'")
      Execute(daemon_cmd)
    else:
      # Waiting for all ps/worker to be exported
      num_ps, num_worker = functions.get_allocated_instances_num()
      num_allocated = num_ps + num_worker
      ps_list, worker_list = functions.get_launched_instances()
      num_launched = len(ps_list) + len(worker_list)
      while num_launched < num_allocated:
        print format("Waiting for all ports({num_launched}/{num_allocated}) to be exported")
        time.sleep(5)
        ps_list, worker_list = functions.get_launched_instances()
        num_launched = len(ps_list) + len(worker_list)
      # Generate parameters
      ps_hosts = ",".join(ps_list)
      worker_hosts = ",".join(worker_list)
      allocated_port = format("{ps_port}") if (componentName == 'ps') else format("{worker_port}")
      task_index = (ps_list.index(format("{hostname}:{ps_port}"))) if (componentName == 'ps') else (
        worker_list.index(format("{hostname}:{worker_port}")))
      mem = mem_ps if (componentName == 'ps') else mem_worker
      # Build clusterSpec and command
      daemon_cmd = format("/usr/bin/docker run -d -u $(id -u yarn) --cgroup-parent={yarn_cg_root}/{container_id} -m {mem}m " \
                   "-v {hadoop_conf}:/usr/local/hadoop/etc/hadoop " \
                   "-v /etc/passwd:/etc/passwd -v /etc/group:/etc/group " \
                   "-v {app_root}:{app_root} -v {app_log_dir}:{app_log_dir} " \
                   "-p {allocated_port}:{allocated_port} --name={container_id} {docker_image} " \
                   "/bin/bash -c 'export HADOOP_USER_NAME={user_name}; /usr/bin/python {app_root}/{user_scripts_entry} " \
                   "--ps_hosts={ps_hosts} --worker_hosts={worker_hosts} --job_name={componentName} --task_index={task_index} " \
                   "--data_dir={user_data_dir} --ckp_dir={checkpoint_dir} >>{app_log_dir}/tensorflow.out 2>&1'")
      Execute(daemon_cmd)
  elif action == 'stop':
    cmd = format("/usr/bin/docker stop {container_id}")
    op_test = format("/usr/bin/docker top {container_id} >/dev/null 2>&1")
    Execute(cmd,
            tries=5,
            try_sleep=10,
            wait_for_finish=True,
            only_if=op_test
            )
  elif action == 'status':
    cmd_status = "/usr/bin/docker inspect -f '{{.State.Running}}' %s" % container_id
    running = os.popen(cmd_status).read().strip('\n')
    if running == 'true':
      print "Component instance is running..."
      # Role tensorboard will watch all workers' status
      if componentName == "tensorboard":
        running, finished = functions.get_workers()
        print "Running tensorflow workers(%s) : %s \nFinished tensorflow workers(%s) : %s" \
              % (len(running), ','.join(running), len(finished), ','.join(finished))
        # All worker has finished successfully, going to stop cluster...
        num_ps, num_worker = functions.get_allocated_instances_num()
        if len(finished) == num_worker:
          functions.stop_cluster()
    else:
      cmd_exit = "/usr/bin/docker inspect -f '{{.State.ExitCode}}' %s" % container_id
      exit_code = int(os.popen(cmd_exit).read().strip('\n'))
      if exit_code != 0:
        retry = functions.set_retry_num(container_id)
        if retry <= 5:
          # Remove failed docker container
          cmd_rm = format("/usr/bin/docker rm -f {container_id}")
          Execute(cmd_rm)
          # restart user tensorflow script
          tensorflow_service(action='start')
        else:
          raise ComponentIsNotRunning()
      else:
        print "Component instance has finished successfully"
        functions.set_container_status(container_id)
