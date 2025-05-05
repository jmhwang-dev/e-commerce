#!/bin/bash
export MY_WORKING_DIR=${PWD}/hadoop

export HADOOP_VERSION=3.4.1
export HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
export HADOOP_CONF_DIR=${MY_WORKING_DIR}/conf

# PATH 설정
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# HDFS 포맷 및 시작
hdfs namenode -format
start-dfs.sh
stop-dfs.sh