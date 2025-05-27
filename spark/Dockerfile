# 베이스 이미지: ARM64 지원 OpenJDK
FROM openjdk:17-slim

# Spark & Hadoop 버전 정의
ARG SPARK_VERSION=3.5.5
ARG HADOOP_VERSION=3.4.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 필수 패키지 설치 및 Spark/Hadoop 다운로드
RUN apt-get update && apt-get install -y curl python3 python3-pip procps && \
    # Spark 다운로드 및 설치
    curl -L https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | \
    tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    # Hadoop client libs 다운로드
    curl -L https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | \
    tar -xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    # PySpark 설치
    pip3 install pyspark && \
    # 환경변수 등록
    echo "export HADOOP_HOME=/opt/hadoop" >> /etc/profile.d/hadoop.sh && \
    echo "export PATH=\$PATH:/opt/hadoop/bin" >> /etc/profile.d/hadoop.sh && \
    echo "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop" >> /etc/profile.d/hadoop.sh && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Hadoop 환경변수
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

WORKDIR $SPARK_HOME