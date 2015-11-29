#! /bin/bash

wget http://apache.websitebeheerjd.nl/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz -O $HOME/kafka_2.10-0.8.2.1.tgz
mvn clean install -DskipTests -Dkafka.src=$HOME/kafka_2.10-0.8.2.1.tgz -Dkafka.version=kafka_2.10-0.8.2.1
unzip -o target/koya-slider-package-0.1.zip appConfig.json
