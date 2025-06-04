# helm install minio minio/minio \ or 
helm upgrade --install minio minio/minio \
  --namespace minio \
  --create-namespace \
  --set mode=standalone \
  --set persistence.enabled=true \
  --set persistence.existingClaim=minio \
  --set existingSecret=minio-secret

# uninstall release
helm uninstall minio -n minio