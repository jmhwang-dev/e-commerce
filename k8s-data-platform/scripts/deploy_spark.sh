#!/bin/bash
# 스크립트 실행 중 오류 발생시 즉시 중단
set -e

echo "🚀 spark-operator 배포 시작..."

# Helm 레포지토리 추가 및 업데이트
# Helm: 쿠버네티스 애플리케이션을 패키지로 관리하는 도구
echo "📦 Helm 레포지토리 추가 중..."
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator  # Spark Operator 저장소
helm repo update  # 최신 차트 정보 업데이트

# 네임스페이스 생성 (리소스 분리를 위한 가상 공간)
echo "🏠 네임스페이스 생성 중..."
kubectl apply -f spark/namespace.yaml   # Spark용 namespace

# GitHub Container Registry 접근을 위한 인증 정보 생성
echo "🔐 GitHub Container Registry Secret 생성..."
read -p "GitHub Username: " GITHUB_USERNAME      # 사용자에게 GitHub 사용자명 입력 요청
read -s -p "GitHub Token: " GITHUB_TOKEN         # 비밀번호는 화면에 표시하지 않음 (-s 옵션)
echo  # 줄바꿈

# kubectl로 Docker 레지스트리 인증 Secret 생성
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \                      # GitHub Container Registry 주소
  --docker-username=$GITHUB_USERNAME \           # GitHub 사용자명
  --docker-password=$GITHUB_TOKEN \              # GitHub Personal Access Token
  --namespace=spark \                            # spark namespace에 생성
  --dry-run=client -o yaml | kubectl apply -f - # 기존에 있으면 업데이트, 없으면 생성

# Spark Operator 배포
echo "⚡ Spark Operator 배포 중..."
kubectl apply -f spark/rbac.yaml        # 권한 설정
kubectl apply -f spark/ghcr-secret.yaml # GitHub 인증 정보
# Helm으로 Spark Operator 설치
helm upgrade --install spark-operator spark-operator/spark-operator -n spark -f spark/values.yaml

# 배포 완료 대기 (Pod가 Ready 상태가 될 때까지 최대 5분 대기)
echo "✅ 배포 상태 확인 중..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=spark-operator -n spark --timeout=300s

echo "🎉 배포 완료!"
echo "Spark 작업 실행: kubectl apply -f spark-jobs/sample-job.yaml"    # 샘플 작업 실행 방법
