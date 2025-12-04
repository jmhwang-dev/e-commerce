# 1. 이미지 다운로드
docker pull apache/spark:latest

# docker network create spark-network

# 2. 스파크 마스터 실행
# 호스트에서 실행
docker stop spark-master
docker rm spark-master

docker run -d \
  --name spark-master \
  --network host \
  apache/spark:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master

# 3. 워커 붙이고 나서
docker exec -it spark-master bash

# 마스터 컨테이너 내부에서 실행
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://192.168.45.190:7077 \
  --executor-memory 1g \
  --total-executor-cores 2 \
  ./examples/jars/spark-examples_2.13-4.0.0.jar \
  100

# local mode
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local[2] \
  --deploy-mode client \
  ./examples/jars/spark-examples_2.13-4.0.0.jar \
  10

# 삭제
docker stop spark-master
docker rm spark-master
