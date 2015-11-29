#! /bin/bash

APPNAME=${1:-koya}
slider install-package --replacepkg --name KOYA --package target/koya-slider-package-0.1.zip
slider stop $APPNAME
slider destroy $APPNAME
slider create $APPNAME --filesystem hdfs://root --queue dev --template appConfig.json --resources resources-default.json




