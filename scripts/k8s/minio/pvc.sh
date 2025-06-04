#!/bin/bash

# Create a hostPath persistent volume for MinIO when no StorageClass exists.
kubectl get storageclass

# Apply PV manifest from the repository
kubectl apply -f ../../../infra/k8s/minio/hostpath-pv.yaml

# Check PVC status
kubectl get pvc -n minio
