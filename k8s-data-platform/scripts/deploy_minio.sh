#!/bin/bash
# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

echo "🚀 minio 배포 시작..."

# Helm 레포지토리 추가 및 업데이트
# Helm: 쿠버네티스 애플리케이션을 패키지로 관리하는 도구
echo "📦 Helm 레포지토리 추가 중..."
helm repo add minio https://charts.min.io/              # MinIO Helm Chart 저장소
helm repo update  # 최신 차트 정보 업데이트

# 네임스페이스 생성 (리소스 분리를 위한 가상 공간)
echo "🏠 네임스페이스 생성 중..."
kubectl apply -f minio/namespace.yaml   # MinIO용 namespace

# MinIO 배포
echo "🗄️  MinIO 배포 중..."
kubectl apply -f minio/pv.yaml      # 저장공간 생성
kubectl apply -f minio/pvc.yaml     # 저장공간 요청
kubectl apply -f minio/secret.yaml  # 인증 정보
# Helm으로 MinIO 설치 (values.yaml 파일의 설정값 사용)
helm upgrade --install minio minio/minio -n minio -f minio/values.yaml

# 배포 완료 대기 (Pod가 Ready 상태가 될 때까지 최대 5분 대기)
echo "✅ 배포 상태 확인 중..."
kubectl wait --for=condition=ready pod -l app=minio -n minio --timeout=300s

echo "🎉 배포 완료!"
echo "MinIO Console: http://localhost:30901 (minioadmin/minioadmin123)"  # 웹 UI 접속 정보
