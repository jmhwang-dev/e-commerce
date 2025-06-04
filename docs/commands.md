# Command Cheat Sheet

This document collects useful shell commands for working with the project and Kubernetes environment. Use these while experimenting locally.

## Kubernetes

```bash
# List all nodes
kubectl get nodes

# Inspect pods in a namespace
kubectl get pods -n minio -o wide

# Port-forward MinIO services
kubectl port-forward svc/minio 9000:9000 -n minio
kubectl port-forward svc/minio-console 9001:9001 -n minio
```

## Helm

```bash
# Install MinIO using Helm
helm upgrade --install minio minio/minio \
  --namespace minio \
  --create-namespace \
  --values infra/k8s/minio/values.yaml

# Uninstall
helm uninstall minio -n minio
```

## Docker

```bash
# Remove all containers and images
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
docker rmi -f $(docker images -aq)
docker network prune -f
docker volume prune -f
```

## HDFS

```bash
# Check directory size
hdfs dfs -du -h <path>

# Copy data from HDFS to local
hadoop fs -copyToLocal /user/hadoop/olist_data ~/olist_data
```
