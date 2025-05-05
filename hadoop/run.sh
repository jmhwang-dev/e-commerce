#!/bin/bash
export MY_WORKING_DIR=${PWD}/hadoop

# HDFS 포맷 및 시작
hdfs namenode -format
start-dfs.sh
# stop-dfs.sh