#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

echo "Deploying Spark Operator..."

# Add Helm repo and update
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm repo update

# Namespace
kubectl apply -f spark/namespace.yaml

# Docker registry secret for GHCR
gh_username="${GHCR_USERNAME}" 
gh_token="${GHCR_TOKEN}"
if [ -z "$gh_username" ] || [ -z "$gh_token" ]; then
  echo "GHCR_USERNAME or GHCR_TOKEN not set in .env"
  exit 1
fi

kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username="$gh_username" \
  --docker-password="$gh_token" \
  --namespace=spark \
  --dry-run=client -o yaml | kubectl apply -f -

# Deploy Spark Operator
kubectl apply -f spark/rbac.yaml
helm upgrade --install spark-operator spark-operator/spark-operator -n spark -f spark/values.yaml

kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=spark-operator -n spark --timeout=300s

echo "Spark Operator deployed."
echo "Submit jobs with: kubectl apply -f spark-jobs/sample-job.yaml"

