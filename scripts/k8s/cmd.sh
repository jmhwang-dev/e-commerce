# Namespace
# ├── Pod
# │   └── Container(s)
# ├── Job / Deployment / StatefulSet / etc. ← (Pod를 생성하는 컨트롤러)
# ├── Service (Pod를 선택하여 네트워크로 노출)
# └── ConfigMap / Secret / PVC / etc.

# | 구성 요소          | 설명                                                          |
# | -------------- | ----------------------------------------------------------- |
# | **Namespace**  | 모든 리소스를 **논리적으로 구분**하는 단위 (ex. `default`, `minio`, `spark`) |
# | **Pod**        | **컨테이너가 실행되는 기본 단위**. 보통 1개 컨테이너 포함                         |
# | **Job**        | 일정 수의 **일회성 작업을 실행**하도록 Pod를 생성/관리                          |
# | **Deployment** | 지속적으로 유지되는 Pod를 관리 (웹 서버 등)                                 |
# | **Service**    | Selector를 통해 Pod를 찾아서 네트워크 접근을 **추상화**                      |
# | **Pod의 소속**    | Pod는 직접 Job/Deployment/StatefulSet 등에 의해 **관리**됨.           |


# minio의 경우
# Namespace: minio
# ├── Deployment: minio
# │   └── Pod: minio-xxxxx
# │       └── Container: quay.io/minio/minio
# ├── Service: minio
# │   └── selector: app=minio → Pod 선택
# └── Secret / PVC / ConfigMap 등

# ✅ 올바른 이해 방식

# Job은 Pod를 생성하고
# Service는 특정 라벨을 가진 Pod에 트래픽을 보냄
# Pod는 Namespace 내에서 배치됨

kubectl get nodes
kubectl apply -f sparkpi-job.yaml

kubectl get jobs
kubectl logs job/spark-pi   # kubectl logs job/spark-pi --tail=10

kubectl get pods -l job-name=spark-pi   # pod이 만든 job 확인
kubectl get pod spark-pi-jb92v -o wide
kubectl get node <nodename> -o jsonpath='{.status.nodeInfo.architecture}'   # 특정 노드의 아키텍쳐 확인

kubectl logs spark-pi2-j6rzf | grep "Pi is" # 예시 확인. 여기서는 spark pi
# kubectl delete -f spark-pi-job.yaml # or kubectl delete job spark-pi


kubectl get pods -n minio -o wide
# NAME                     READY   STATUS    RESTARTS      AGE   IP           NODE           NOMINATED NODE   READINESS GATES
# minio-65956b458c-4j2sq   1/1     Running   0             21m   10.244.1.5   raspberrypi2   <none>           <none>
# minio-post-job-k45tm     1/1     Running   1 (10m ago)   21m   10.244.2.4   raspberrypi    <none>           <none>


kubectl get svc -n minio    # svc: service의 약자
# NAME            TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
# minio           ClusterIP   10.97.164.21    <none>        9000/TCP   42s
# minio-console   ClusterIP   10.100.80.248   <none>        9001/TCP   42s

kubectl describe svc minio -n minio
# Name:                     minio
# Namespace:                minio
# Labels:                   app=minio
#                           app.kubernetes.io/managed-by=Helm
#                           chart=minio-5.4.0
#                           heritage=Helm
#                           monitoring=true
#                           release=minio
# Annotations:              meta.helm.sh/release-name: minio
#                           meta.helm.sh/release-namespace: minio
# Selector:                 app=minio,release=minio
# Type:                     ClusterIP
# IP Family Policy:         SingleStack
# IP Families:              IPv4
# IP:                       10.97.164.21
# IPs:                      10.97.164.21
# Port:                     http  9000/TCP
# TargetPort:               9000/TCP
# Endpoints:                10.244.1.5:9000
# Session Affinity:         None
# Internal Traffic Policy:  Cluster
# Events:                   <none>

kubectl get pods -n minio -l app=minio -o wide
# NAME                     READY   STATUS    RESTARTS   AGE   IP           NODE           NOMINATED NODE   READINESS GATES
# minio-65956b458c-4j2sq   1/1     Running   0          23m   10.244.1.5   raspberrypi2   <none>           <none>

# =============================
# API
kubectl port-forward svc/minio 9000:9000 -n minio
curl -u id:pw http://localhost:9000/minio/health/ready

# Web UI
kubectl port-forward svc/minio-console 9001:9001 -n minio
curl -I http://localhost:9001

# 아래 결과 확인 후에
kubectl get secret -n minio minio -o yaml

# 디코딩 해서 실제값 확인
echo "rootUser" | base64 -d
echo "rootPassword" | base64 -d

# 비밀번호 변경하려면
helm upgrade minio minio/minio \
  --namespace minio \
  --set accessKey=minioadmin \
  --set secretKey=minioadmin
# =============================
