Kafka On YARN (KOYA)
====================

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/DataTorrent/koya?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

### Goals

  * Use capabilities of YARN for Kafka broker management
  * Automate broker recovery
  * Make it easy to deploy, configure and monitor Kafka clusters
  * Simplify management tasks (alternative to Kafka command line utilities)

JIRA: https://issues.apache.org/jira/browse/KAFKA-1754

Kafka as YARN application using Slider
-----------------------------------------------

### Build

Checkout Slider code (https://github.com/apache/incubator-slider) 
```sh
git clone git@github.com:apache/incubator-slider.git
git checkout -b slider-0.80.0-incubating remotes/origin/releases/slider-0.80.0-incubating
```
The Slider version you checked out needs to match ${slider.version} in pom.xml

Create symbolic link to Slider source code within the KOYA repository:
```sh
ln -s /path/to/repo/incubator-slider/ slider
```
Download Kafka binary package (http://kafka.apache.org/downloads.html)

```sh
mvn clean install -DskipTests -Dkafka.src=path/to/kafka_2.10-0.8.1.1.tgz -Dkafka.version=kafka_2.10-0.8.1.1
```
Artifacts:

 - Archive with embedded Slider: __`target/koya-with-slider.zip`__
 - Separate Slider application package: __`target/koya-slider-package-0.1.zip`__

###Installation

####Install Slider

To use the archive with embedded Slider, copy it to the machine from which you launch YARN applications (Hadoop client, gateway or edge node). Extract the file and configure Slider:

If the environment variables `HADOOP_CONF_DIR` or `JAVA_HOME` are not already defined through your Hadoop installation, you can export them in  `slider-0.80.0-incubating/conf/slider-env.sh` 

Example for CDH 5.4:
``` 
export HADOOP_CONF_DIR=/etc/hadoop/conf
export JAVA_HOME=/usr/java/jdk1.7.0_45-cloudera
```
If the registry ZooKeeper quorum was not already configured through Hadoop, modify `slider-0.80.0-incubating/conf/slider-client.xml`: 
```
  <property>
    <name>hadoop.registry.zk.quorum</name>
    <value>node26:2181,node27:2181,node28:2181</value>
  </property>
```
Above steps are not required with HDP 2.2

More information regarding Slider client configuration refer to http://slider.incubator.apache.org/docs/client-configuration.html

### Configure KOYA application package

Before the Kafka cluster can be launched, the brokers need to be defined. Currently Slider does not support [configuration properties at instance level](https://issues.apache.org/jira/browse/SLIDER-851), therefore each broker has to be configured as a component.

If you use the full archive, the configuration file templates are already in your working directory. Otherwise extract them from the Slider package.

####appConfig.json

Extract the packaged configuration files you are going to customize:
```
unzip koya-slider-package-0.1.zip appConfig.json resources.json 
```
Adjust following properties in the global section: 
```
    "application.def": "koya-slider-package-0.1.zip",
    "site.global.xmx_val": "256m",
    "site.global.xms_val": "128m",
    "site.broker.zookeeper.connect": "${ZK_HOST}"
```
Above will be used to configure server.properties and launch the Kafka server(s). All properties prefixed with `site.broker.` will be set in the server.properties file supplied with the Kafka archive. Only non-default settings need to be defined here.  

####resources.json

Configure the number of servers and other resource requirements:
```
  "components" : {
    "broker" : {
      "yarn.role.priority" : "1",
      "yarn.component.instances" : "5",
      "yarn.memory" : "768",
      "yarn.vcores" : "1",
      "yarn.component.placement.policy":"1"
    }
```
More information about the application configuration can be found [here](http://slider.incubator.apache.org/docs/configuration/core.html).

### Deploy KOYA Cluster

The Slider application package needs to be copied to the HDFS location that was specified as application.def in appConfig.json:
```
hdfs dfs -copyFromLocal koya-slider-package-0.1.zip /path/in/appConfig
```
Now the KOYA cluster can be deployed and launched:
```
slider-0.80.0-incubating/bin/slider create koya --template ~/koya/appConfig.json  --resources ~/koya/resources.json
```
