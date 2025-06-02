# create secretd
kubectl create secret generic minio-credentials \
  --from-literal=accesskey=minioadmin \
  --from-literal=secretkey=minioadmin \
  -n minio

# check
kubectl get secret minio-credentials -n minio