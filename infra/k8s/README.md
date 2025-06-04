# K8s Data Platform

Kubernetes 기반 데이터 플랫폼으로 MinIO(Object Storage)와 Spark를 활용한 데이터 파이프라인 구축

## 🏗️ 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│     MinIO       │───▶│  Spark Jobs     │
│                 │    │ (Object Storage)│    │ (Data Pipeline) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📋 구성 요소

- **MinIO**: S3 호환 객체 스토리지 (데이터 레이크)
- **Spark**: 분산 데이터 처리 엔진
- **Spark Operator**: Kubernetes에서 Spark 작업 관리

## 🚀 빠른 시작

### 1. 배포
```bash
cp ../.env.example ../.env # 환경 변수 설정
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
│   ├── namespace.yaml  # MinIO 전용 네임스페이스 정의
│   ├── pv.yaml         # 영구 저장소 볼륨 정의
│   ├── pvc.yaml        # 저장소 요청 정의
│   └── values.yaml     # MinIO Helm Chart 설정값
├── spark/              # Spark Operator 설정
│   ├── namespace.yaml  # Spark 전용 네임스페이스 정의
│   ├── rbac.yaml       # 권한 설정 (ServiceAccount, Role, ClusterRole 등)
│   └── values.yaml     # Spark Operator Helm Chart 설정값
├── spark-jobs/         # Spark 작업 정의
│   ├── sample-job.yaml # 기본 예제 작업 (SparkPi 계산)
│   ├── etl-job.yaml    # ETL 파이프라인 작업
│   └── olist-analysis.yaml # 이커머스 데이터 분석 작업
├── scripts/            # 배포/정리 스크립트
│   ├── deploy.sh       # 전체 플랫폼 자동 배포 스크립트
│   └── cleanup.sh      # 전체 플랫폼 정리(삭제) 스크립트
└── README.md          # 이 문서
```

## 🔐 GitHub Container Registry 사용

본인의 커스텀 Spark 이미지를 사용하려면:

1. **spark-jobs/*.yaml 파일에서 이미지 경로 수정:**
   ```yaml
   image: ghcr.io/YOUR_USERNAME/spark-custom:latest
   ```

2. **GitHub Token 권한 설정:**
   - GitHub Settings → Developer settings → Personal access tokens
   - `read:packages` 권한이 있는 토큰 생성
   - 배포 스크립트 실행 시 사용자명과 토큰 입력

## 🛠️ 개발 가이드

### 새로운 Spark 작업 추가
1. `spark-jobs/` 디렉토리에 새 YAML 파일 생성
2. MinIO에 Python/Scala 코드 업로드
3. `kubectl apply -f spark-jobs/your-job.yaml` 실행

### 데이터 업로드 (MinIO)
```bash
# MinIO Client 설치 후
mc alias set local http://localhost:30900 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc cp your-data.csv local/data-lake/raw/
```

### 작업 상태 모니터링
```bash
# 모든 Spark 작업 확인
kubectl get sparkapplications -n spark

# 특정 작업 상세 정보
kubectl describe sparkapplication sample-job -n spark

# Driver Pod 로그 확인
kubectl logs -f <driver-pod-name> -n spark

# Spark UI 접근 (포트포워딩)
kubectl port-forward <driver-pod-name> 4040:4040 -n spark
# 브라우저에서 http://localhost:4040 접속
```

## 🧹 정리
```bash
./scripts/cleanup.sh
```

## 💡 실무 팁

### 리소스 최적화
- **CPU/Memory 설정**: 데이터 크기에 맞게 driver/executor 리소스 조정
- **Executor 개수**: 데이터 파티션 수와 병렬 처리 수준 고려
- **메모리 설정**: 큰 데이터셋의 경우 executor memory 증가

### 모니터링 및 디버깅
- **Spark UI**: 각 작업의 실행 계획과 성능 메트릭 확인
- **Event Log**: MinIO의 spark-logs 버킷에서 이력 확인
- **Kubernetes 로그**: kubectl logs로 Pod 수준 로그 확인

### 배치 스케줄링
```yaml
# CronJob과 연동하여 정기 실행
apiVersion: batch/v1
kind: CronJob
metadata:
  name: daily-etl
spec:
  schedule: "0 2 * * *"  # 매일 새벽 2시 실행
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: spark-submit
            image: bitnami/kubectl
            command: ["kubectl", "apply", "-f", "spark-jobs/etl-job.yaml"]
```

## 📊 사용 사례

### 1. ETL 파이프라인
```
Raw Data (CSV/JSON) → MinIO → Spark ETL → Processed Data → MinIO
```

### 2. 실시간 분석
```
Kafka → Spark Streaming → MinIO → Dashboard
```

### 3. 머신러닝 파이프라인
```
Training Data → Spark MLlib → Model → MinIO → Serving
```

### 4. 데이터 품질 검증
```
Data Source → Spark + Great Expectations → Quality Report → MinIO
```

## 🔧 고급 설정

### 네트워크 정책 (보안 강화)
```yaml
# spark namespace 간 통신만 허용
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-network-policy
  namespace: spark
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: spark
```

### 리소스 쿼터 (리소스 제한)
```yaml
# spark namespace의 총 리소스 사용량 제한
apiVersion: v1
kind: ResourceQuota
metadata:
  name: spark-quota
  namespace: spark
spec:
  hard:
    requests.cpu: "10"      # 최대 10 CPU 코어
    requests.memory: 20Gi   # 최대 20GB 메모리
    pods: "50"              # 최대 50개 Pod
```

## 🚨 주의사항

1. **NodePort 보안**: 프로덕션에서는 LoadBalancer나 Ingress 사용 권장
2. **데이터 백업**: MinIO 데이터는 별도 백업 전략 필요
3. **리소스 모니터링**: 클러스터 리소스 사용량 지속 모니터링
4. **Secret 관리**: 실제 운영시 Secret을 코드에 하드코딩하지 말고 별도 관리

## 📚 참고 자료

- [Spark on Kubernetes 공식 문서](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [MinIO Kubernetes 가이드](https://min.io/docs/minio/kubernetes/upstream/)
- [Spark Operator GitHub](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)
