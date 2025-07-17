#!/bin/bash
set -e

echo "[INIT] Fixing ownership..."
chown -R appuser:appuser /var/lib/kafka/data

echo "[INIT] Formatting with Cluster ID: ${CLUSTER_ID}"
if [ ! -f /var/lib/kafka/data/meta.properties ]; then
  exec gosu appuser /opt/kafka/bin/kafka-storage.sh format \
    --cluster-id "${CLUSTER_ID}" \
    --config /etc/kafka/server.properties
fi
