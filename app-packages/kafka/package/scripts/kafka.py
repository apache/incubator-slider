import logging
import sys
import os
import inspect
import pprint
import util
from resource_management import *

logger = logging.getLogger()

class Kafka(Script):
  def install(self, env):
    self.install_packages(env)

  def configure(self, env):
    import params
    env.set_params(params)

  def start(self, env):
    import params
    env.set_params(params)
    self.configure(env)

    # log the component configuration
    ppp = pprint.PrettyPrinter(indent=4)
    logger.info("broker component config: " + ppp.pformat(params.broker_config))

    # log the environment variables
    logger.info("Env Variables:")
    for key in os.environ.keys():
      logger.info("%10s %s \n" % (key,os.environ[key]))
    pass

    # This updating thing is changing files in-place and it really
    # should not (static cache)

    # For kafka 0.8.1.1, there is no way to set the log dir to location other than params.app_root + "/logs"
    if(params.kafka_version.find("0.8.1.1") != -1):
      os.symlink(params.app_root + "/logs", params.app_log_dir + "/kafka")
    else:
      kafkaLogConfig = {"kafka.logs.dir" : params.app_log_dir + "/kafka"}
      util.updating(params.app_root + "/config/log4j.properties", kafkaLogConfig)
#      File(format("{params.app_root}/conf/log4j.properties"),
#           owner=params.app_user,
#           content=InlineTemplate(param.log4j_prop))
    pass

    # update the broker properties for different brokers
    server_conf=format("{params.conf_dir}/server.slider.properties")
    PropertiesFile(server_conf, properties = params.broker_config, owner=params.app_user)

    # execute the process
    process_cmd = format("{app_root}/bin/kafka-server-start.sh {server_conf}")
    os.environ['LOG_DIR'] = params.app_log_dir + "/kafka"
    HEAP_OPT = ""
    if params.xmx:
        HEAP_OPT = HEAP_OPT + " -Xmx" + params.xmx
    pass
    if params.xms:
        HEAP_OPT = HEAP_OPT + " -Xms" + params.xms
    pass
    if HEAP_OPT:
        os.environ['KAFKA_HEAP_OPTS'] = HEAP_OPT
    pass
    Execute(process_cmd,
        user=params.app_user,
        logoutput=True,
        wait_for_finish=False,
        pid_file=params.pid_file
    )

  def stop(self, env):
    import params
    env.set_params(params)
    pid = format("`cat {pid_file}` >/dev/null 2>&1")
    Execute(format("kill {pid}"),
      user=params.app_user
    )
    Execute(format("kill -9 {pid}"),
      ignore_failures=True,
      user=params.app_user
    )
    Execute(format("rm -f {pid_file}"),
      user=params.app_user)

  def status(self, env):
    import status_params
    env.set_params(status_params)
##    jps_cmd = format("{java64_home}/bin/jps")
    no_op_test = format("ls {pid_file} >/dev/null 2>&1 && ps `cat {pid_file}` >/dev/null 2>&1")
##    cmd = format("echo `{jps_cmd} | grep Kafka | cut -d' ' -f1` > {pid_file}")
##    Execute(cmd, not_if=no_op_test)
    check_process_status(status_params.pid_file)

if __name__ == "__main__":
  Kafka().execute()
