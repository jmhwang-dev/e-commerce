# 현재 클러스터에는 StorageClass가 하나도 없기 때문에,
# Helm이 PVC를 요청해도 자동으로 볼륨을 만들 수 없어서, 프로비저너가 실패
kubectl get storageclass    # 결과 없음

# 직접 pv용 디렉토리 생성 후 pod 생성
kubectl apply -f ../k8s/minio-hostpath-pv.yaml

# 확인
kubectl get pvc -n minio