#!/bin/bash

# Port-forward MinIO services to the local machine.

# Web UI
kubectl port-forward svc/minio-console 9001:9001 -n minio &

# API
kubectl port-forward svc/minio 9000:9000 -n minio
