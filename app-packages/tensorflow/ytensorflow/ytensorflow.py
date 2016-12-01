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

import os
import sys
import json


def main():
  """
  ytensorflow main method
  :return: exit code of the process
  """
  returncode = 1
  slider_home = sys.path[0] + '/../../..'
  slider = slider_home + '/bin/slider' if os.path.exists(slider_home + '/bin/slider') else 'slider'
  args = sys.argv[1:]
  if args[0] == 'version':
    print get_version()
    returncode = 0
  elif args[0] == 'cluster':
    if args[1] == '-start':
      app_name = parse_conf(args[2])
      if 4 < len(args):
        cp_files(args[args.index('-files') + 1])
      cmd = "%s status %s || %s destroy %s --force && %s create %s --appdef %s --resources %s --template %s" \
            %(slider, app_name,
              slider, app_name,
              slider, app_name, sys.argv[-1], sys.argv[-1] + '/resources.json', sys.argv[-1] + '/appConfig.json')
    if args[1] == '-stop':
      app_name = args[2]
      cmd = slider + " stop " + app_name
    if args[1] == '-status':
      app_name = args[2]
      cmd = slider + " status " + app_name
    print cmd
    returncode = os.system(cmd)
  return returncode

def get_version():
  root_path = sys.path[0] + '/..'
  with open(root_path + "/metainfo.json", 'r') as f:
    metainfo = json.load(f)
    name = metainfo['application']['name']
    version = metainfo['application']['version']
  return name + " on Slider " + version

def cp_files(files):
  os.system('cp -r ' + files + ' ' + sys.argv[-1] + '/package/files/')

def parse_conf(config_file):
  root_path = sys.path[0] + '/..'
  tmp_path = sys.argv[-1]
  app_name = 'default_app_name'
  with open(config_file, 'r') as f:
    data = json.load(f)
    for k in data:
      if k == 'commandConfig':
        app_name = data[k]['app.name']
      if k == 'appConfig':
        # override appconfig.default.json
        with open(root_path + '/appConfig.default.json', 'r') as f:
          data_app = json.load(f)
          for kk,vv in data['appConfig']['global'].items():
            data_app['global']["site.global." + kk] = vv
        with open(tmp_path + '/appConfig.json', 'w') as f:
          json.dump(data_app, f)
      if k == 'resources':
        # override resources.default.json
        with open(root_path + '/resources.default.json', 'r') as f:
          data_res = json.load(f)
          for kk,vv in data['resources']['components']['ps'].items():
            data_res['components']['ps'][kk] = vv
          for kk,vv in data['resources']['components']['chiefworker'].items():
            data_res['components']['chiefworker'][kk] = vv
          for kk,vv in data['resources']['components']['worker'].items():
            data_res['components']['worker'][kk] = vv
        with open(tmp_path + '/resources.json', 'w') as f:
          json.dump(data_res, f)
  return app_name

if __name__ == '__main__':
  """
  Entry point
  """
  try:
    returncode = main()
  except Exception as e:
    print "Exception: %s " % str(e)
    returncode = -1
  sys.exit(returncode)
