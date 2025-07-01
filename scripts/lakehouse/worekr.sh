docker pull apache/spark:latest

# 스파크 워커 실행 (마스터와 연결)
docker run -d \
  --name spark-worker-1 \
  --network host \
  apache/spark:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  spark://192.168.45.192:7077

# 삭제
docker stop spark-master
docker rm spark-master
