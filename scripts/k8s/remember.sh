kubectl get nodes
kubectl apply -f sparkpi-job.yaml

kubectl get jobs
kubectl logs job/spark-pi   # kubectl logs job/spark-pi --tail=10

kubectl get pods -l job-name=spark-pi   # pod이 만든 job 확인
kubectl get pod spark-pi-jb92v -o wide
kubectl get node <nodename> -o jsonpath='{.status.nodeInfo.architecture}'   # 특정 노드의 아키텍쳐 확인

kubectl logs spark-pi2-j6rzf | grep "Pi is" # 예시 확인. 여기서는 spark pi
# kubectl delete -f spark-pi-job.yaml # or kubectl delete job spark-pi