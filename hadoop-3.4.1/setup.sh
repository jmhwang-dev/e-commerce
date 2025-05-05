#!/bin/bash

export HADOOP_VERSION=3.4.1
. ./hadoop-${HADOOP_VERSION}/etc/hadoop/hadoop-env.sh
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH