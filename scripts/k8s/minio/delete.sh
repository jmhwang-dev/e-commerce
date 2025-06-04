#!/bin/bash

# Remove the MinIO namespace and persistent volume.
kubectl delete namespace minio
kubectl delete pv minio-pv
