#!/bin/bash
# 전체 데이터 플랫폼을 정리(삭제)하는 스크립트

set -e

echo "🧹 K8s Data Platform 정리 시작..."

# 실행 중인 모든 Spark 작업 삭제
echo "⚡ Spark 작업 삭제 중..."
kubectl delete sparkapplications --all -n spark || true  # || true: 실패해도 스크립트 계속 진행

# Helm으로 설치한 애플리케이션들 제거
echo "📦 Helm 릴리스 삭제 중..."
helm uninstall spark-operator -n spark || true  # Spark Operator 제거
helm uninstall minio -n minio || true           # MinIO 제거

# 수동으로 생성한 쿠버네티스 리소스들 삭제
echo "🗑️  리소스 삭제 중..."
kubectl delete -f spark/rbac.yaml || true    # Spark 권한 설정 삭제
kubectl delete -f minio/ || true             # MinIO 관련 모든 리소스 삭제

# 네임스페이스 삭제 (해당 namespace의 모든 리소스도 함께 삭제됨)
echo "🏠 네임스페이스 삭제 중..."
kubectl delete namespace spark || true  # Spark namespace 삭제
kubectl delete namespace minio || true  # MinIO namespace 삭제

# PersistentVolume은 네임스페이스와 독립적이므로 별도 삭제
kubectl delete pv minio-pv || true

echo "✅ 정리 완료!"