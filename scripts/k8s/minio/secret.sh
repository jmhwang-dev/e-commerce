#!/bin/bash
# 기존 시크릿 삭제 (있을 경우)
kubectl delete secret minio-credentials -n minio --ignore-not-found

# 시크릿 생성 (Helm 차트에서 요구하는 키 이름 사용)
kubectl create secret generic minio-credentials \
  --namespace minio \
  --from-literal=rootUser=7JqfySFS6u10HmELYhfq \
  --from-literal=rootPassword=LavELPnELnEIbq6xLcL2LGJwWqKWypSdCLN8k0um

# check
kubectl get secret minio-credentials -n minio