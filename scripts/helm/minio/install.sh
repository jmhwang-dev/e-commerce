# 1. Helm GPG 키 추가
curl https://baltocdn.com/helm/signing.asc | sudo tee /etc/apt/trusted.gpg.d/helm.asc

echo "deb https://baltocdn.com/helm/stable/debian/ all main" | \
  sudo tee /etc/apt/sources.list.d/helm-stable-debian.list

sudo apt update
sudo apt install -y helm  # /usr/bin/helm 으로 설치됨
# helm version  # Current version: v3.17.3

helm repo add minio https://charts.min.io/
helm repo update
helm search repo minio  # Current version: 5.4.0

helm install minio minio/minio \
  --namespace minio \
  --create-namespace \
  --set accessKey=minioadmin \
  --set secretKey=minioadmin \
  --set mode=standalone \
  --set persistence.enabled=false \
  --set resources.requests.memory=512Mi

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