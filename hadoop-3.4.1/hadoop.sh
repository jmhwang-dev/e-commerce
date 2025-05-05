#!/bin/bash

HADOOP_VERSION=3.4.1
export HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
export HADOOP_CONF_DIR=$PWD/hadoop-$HADOOP_VERSION/etc/hadoop
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

ARCH=$(uname -m)
if [[ "$ARCH" == "x86_64" ]]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [[ "$ARCH" == "aarch64" ]]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64
else
    echo "Unsupported operating system"
    exit 1
fi

export JAVA_HOME
export LANG=en_US.UTF-8