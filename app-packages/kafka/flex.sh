#! /bin/bash

APPNAME=${1:-koya}
APPNUM=${2:-12}
slider flex $APPNAME --component BROKER0 $APPNUM --filesystem hdfs://root
