# web ui
kubectl port-forward svc/minio-console 9001:9001 -n minio

# API
kubectl port-forward svc/minio 9000:9000 -n minio
