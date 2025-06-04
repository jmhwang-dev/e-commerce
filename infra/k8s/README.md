# K8s Data Platform

Kubernetes 기반 데이터 플랫폼으로 MinIO(Object Storage)와 Spark를 활용한 데이터 파이프라인을 구축합니다.

## 🏗️ 아키텍처
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│     MinIO       │───▶│  Spark Jobs     │
│                 │    │ (Object Storage)│    │ (Data Pipeline) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📋 구성 요소
- **MinIO**: S3 호환 객체 스토리지
- **Spark**: 분산 데이터 처리 엔진
- **Spark Operator**: Kubernetes에서 Spark 작업 관리

## 🚀 빠른 시작
### 1. 배포
```bash
cp ../../.env.example .env  # 환경 변수 설정
chmod +x scripts/deploy_all.sh
./scripts/deploy_all.sh
```
### 2. MinIO 웹 콘솔 접속
- URL: http://localhost:30901
- 계정: `.env` 파일에 설정한 관리자 계정

### 3. 샘플 Spark 작업 실행
```bash
kubectl apply -f spark-jobs/sample-job.yaml
```

### 4. 작업 상태 확인
```bash
kubectl get sparkapplications -n spark
```

## 📁 디렉토리 구조
```
infra/k8s/
├── minio/              # MinIO 설정
│   ├── namespace.yaml
│   ├── pv.yaml
│   ├── pvc.yaml
│   └── values.yaml
├── spark/              # Spark Operator 설정
│   ├── namespace.yaml
│   ├── rbac.yaml
│   └── values.yaml
├── spark-jobs/         # Spark 작업 정의
│   ├── sample-job.yaml
│   ├── etl-job.yaml
│   └── olist-analysis.yaml
├── scripts/            # 배포 및 정리 스크립트
│   ├── deploy_all.sh
│   ├── deploy_minio.sh
│   ├── deploy_spark.sh
│   └── cleanup.sh
└── README.md
```

## 🔐 GitHub Container Registry 사용
1. `spark-jobs/*.yaml` 파일에서 이미지 경로를 수정합니다.
2. `.env` 파일에 `GHCR_USERNAME`과 `GHCR_TOKEN`을 설정합니다.

