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

import json
import urllib2
import socket
from kazoo.client import KazooClient
from resource_management import *


def get_am_rest_base():
  am_rest_base = ""
  zk = KazooClient(hosts=format("{registry_zk}"))
  zk.start()
  path = format("/registry/users/{user_name}/services/org-apache-slider/{service_name}")
  data, stat = zk.get(path)
  app_registry = json.loads(data)
  for item in app_registry['external']:
    if item['api'] == 'classpath:org.apache.slider.client.rest':
      am_rest_base = item['addresses'][0]['uri']
  return am_rest_base


def get_allocated_resources():
  resources_rest_url = get_am_rest_base() + '/ws/v1/slider/application/live/resources'
  resources = json.loads(urllib2.urlopen(resources_rest_url).read())
  mem_ps = int(resources['components']['ps']['yarn.memory'])
  vcore_ps = int(resources['components']['ps']['yarn.vcores'])
  mem_chiefworker = int(resources['components']['chiefworker']['yarn.memory'])
  vcore_chiefworker = int(resources['components']['chiefworker']['yarn.vcores'])
  mem_worker = int(resources['components']['worker']['yarn.memory'])
  vcore_worker = int(resources['components']['worker']['yarn.vcores'])
  mem_tb = int(resources['components']['tensorboard']['yarn.memory'])
  vcore_tb = int(resources['components']['tensorboard']['yarn.vcores'])
  dict = {"mem.ps": mem_ps, "vcore.ps": vcore_ps,
          "mem.chiefworker": mem_chiefworker, "vcore_chiefworker": vcore_chiefworker,
          "mem.worker": mem_worker, "vcore.worker": vcore_worker,
          "mem.tensorboard": mem_tb, "vcore.tensorboard": vcore_tb}
  return dict

def get_allocated_instances_num():
  resources_rest_url = get_am_rest_base() + '/ws/v1/slider/application/live/resources'
  resources = json.loads(urllib2.urlopen(resources_rest_url).read())
  n = int(resources['components']['ps']['yarn.component.instances'])
  cw = int(resources['components']['chiefworker']['yarn.component.instances'])
  w = int(resources['components']['worker']['yarn.component.instances'])
  return n, cw + w

def get_launched_instances():
  try:
    exports_rest_url = get_am_rest_base() + '/ws/v1/slider/publisher/exports'
    # get launched ps list
    exports_ps = json.loads(urllib2.urlopen(exports_rest_url + '/ps').read())
    ps_list = []
    for item in exports_ps['entries']['host_port']:
      ps_list.append(item['value'])
    # get launched chief worker
    exports_chiefworker = json.loads(urllib2.urlopen(exports_rest_url + '/chiefworker').read())
    chiefworker_list = []
    for item in exports_chiefworker['entries']['host_port']:
      chiefworker_list.append(item['value'])
    # get launched worker list
    exports_worker = json.loads(urllib2.urlopen(exports_rest_url + '/worker').read())
    worker_list = []
    for item in exports_worker['entries']['host_port']:
      worker_list.append(item['value'])
  except:
    return ([], [])
  else:
    return (ps_list, chiefworker_list + worker_list)

def get_application_id(container_id):
  ss = container_id.split('_')
  return "application_" + ss[-4] + "_" + ss[-3]

def is_port_active(host, port, retry=3):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  for i in range(0, 3):
    try:
      sock.connect((host, port))
      sock.close()
      return True
    except Exception, e:
      sock.close()
  return False


def get_workers():
  running = []
  finished = []
  comps_url = get_am_rest_base() + format(
    "/ws/v1/slider/registry/users/{user_name}/services/org-apache-slider/{service_name}/components")
  comps = json.loads(urllib2.urlopen(comps_url).read())
  for node in comps['nodes']:
    comp_url = comps_url + "/" + node
    comp = json.loads(urllib2.urlopen(comp_url).read())
    if 'worker' in comp['service']['description']:
      if comp['service'].has_key('status') and comp['service']['status'] == 'finished':
        finished.append(node)
      else:
        running.append(node)
  return running, finished


def set_container_status(containerid, status='finished'):
  comp_url = get_am_rest_base() \
             + format("/ws/v1/slider/registry/users/{user_name}/services/org-apache-slider/{service_name}/components/") \
             + containerid.replace('_', '-')
  comp = json.loads(urllib2.urlopen(comp_url).read())
  if not comp['service'].has_key('status') or comp['service']['status'] != 'finished':
    comp['service']['status'] = status
    zk = KazooClient(hosts=format("{registry_zk}"))
    zk.start()
    path = format("/registry/users/{user_name}/services/org-apache-slider/{service_name}/components/") \
           + containerid.replace('_', '-')
    zk.set(path, json.dumps(comp['service']))


def set_retry_num(containerid):
  comp_url = get_am_rest_base() \
             + format("/ws/v1/slider/registry/users/{user_name}/services/org-apache-slider/{service_name}/components/") \
             + containerid.replace('_', '-')
  comp = json.loads(urllib2.urlopen(comp_url).read())
  if not comp['service'].has_key('retry'):
    comp['service']['retry'] = 1
  else:
    comp['service']['retry'] = int(comp['service']['retry']) + 1
  zk = KazooClient(hosts=format("{registry_zk}"))
  zk.start()
  path = format("/registry/users/{user_name}/services/org-apache-slider/{service_name}/components/") \
         + containerid.replace('_', '-')
  zk.set(path, json.dumps(comp['service']))
  return comp['service']['retry']


def stop_cluster():
  # use restAPI to stop cluster
  stop_url = get_am_rest_base() + '/ws/v1/slider/application/action/stop'
  # To be compatible with hadoop-2.6.*, make a trick, use GET method to stop
  # This will be replaced by POST, https://issues.apache.org/jira/browse/YARN-2031
  # And actionStopGet should be implemented in ApplicationResource.java
  res = urllib2.urlopen(stop_url).read()
  print res
