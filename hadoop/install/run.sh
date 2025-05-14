#!/bin/bash
source ./hadoop/install/config.sh
source ./hadoop/install/java.sh
source ./hadoop/install/hadoop.sh
source ./hadoop/install/profile.sh

HOSTNAME=$(hostname)

if [[ "$HOSTNAME" == "mini-pc" || $HOSTNAME =~ ^raspberrypi ]]; then
    sudo mkdir -p /mnt/hadoop/dfs/name
    sudo mkdir -p /mnt/hadoop/dfs/data
    sudo chown -R jmhwang:jmhwang /mnt/hadoop
    echo "[DONE] HDFS cluster has configured."
fi

echo "[DONE] HDFS has configured."