# 1. Helm GPG 키 추가
curl https://baltocdn.com/helm/signing.asc | sudo tee /etc/apt/trusted.gpg.d/helm.asc

# 2. Helm 저장소 추가
echo "deb https://baltocdn.com/helm/stable/debian/ all main" | \
  sudo tee /etc/apt/sources.list.d/helm-stable-debian.list

# 3. 패키지 목록 갱신
sudo apt update

# 4. Helm 설치
sudo apt install -y helm


# /usr/bin/helm 으로 설치됨
# Current version: v3.17.3
# helm version

helm repo add minio https://charts.min.io/
helm repo update

# Current version: 5.4.0
helm search repo minio

# For test on local
# [터미널 (컨트롤 플레인)]
#     │
#     ▼
# [helm install -> Kubernetes API 서버]
#     │
#     ▼
# [Kubernetes Scheduler가 실행할 노드를 선택]
#     │
#     ▼
# [워커 노드에 파드가 배치됨]
helm install minio minio/minio \
  --namespace minio \
  --create-namespace \
  --set accessKey=minioadmin \
  --set secretKey=minioadmin \
  --set mode=standalone \
  --set persistence.enabled=false \
  --set resources.requests.memory=512Mi

# 삭제
# kubectl delete namespace minio
# helm uninstall minio -n minio

