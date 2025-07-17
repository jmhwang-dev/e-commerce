#!/bin/bash
set -e

echo "Formatting kafka1 with Cluster ID: ${CLUSTER_ID}"

if [ ! -f /var/lib/kafka/data/meta.properties ]; then
  exec gosu appuser /opt/kafka/bin/kafka-storage.sh format \
    --cluster-id "${CLUSTER_ID}" \
    --config /etc/kafka/server.properties
fi
