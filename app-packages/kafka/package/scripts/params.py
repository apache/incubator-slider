from resource_management import *

config = Script.get_config()


app_root = config['configurations']['global']['app_root']
java64_home = config['hostLevelParams']['java_home']
app_user = config['configurations']['global']['app_user']
pid_file = config['configurations']['global']['pid_file']
app_log_dir = config['configurations']['global']['app_log_dir']
kafka_version = config['configurations']['global']['kafka_version']
xmx = config['configurations']['broker']['xmx_val']
xms = config['configurations']['broker']['xms_val']

conf_dir = format("{app_root}/config")

broker_config=dict(line.strip().split('=') for line in open(format("{conf_dir}/server.properties")) if not (line.startswith('#') or re.match(r'^\s*$', line)))
broker_config.update(config['configurations']['server'])
