# create secretd
kubectl create secret generic minio-credentials \
  --from-literal=accesskey= \
  --from-literal=secretkey= \
  -n minio

# check
kubectl get secret minio-credentials -n minio
