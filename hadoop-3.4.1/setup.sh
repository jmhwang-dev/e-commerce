#!/bin/bash

HADOOP_HOME=/opt/hadoop-3.4.1
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