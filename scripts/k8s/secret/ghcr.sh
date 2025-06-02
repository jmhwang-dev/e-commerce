# create secretd
kubectl create secret docker-registry ghcr-creds \
  --docker-username=jmhwang-dev \
  --docker-password=ghp_xxxxxxxxxxxxxxxxxxxxx \
  --docker-server=ghcr.io \
  --namespace=default

# check
kubectl get secret ghcr-creds -n default