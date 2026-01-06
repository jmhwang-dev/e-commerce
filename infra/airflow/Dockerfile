# ==============================================================================
# STAGE 1: 빌더 스테이지 (Builder Stage)
# ==============================================================================
ARG AIRFLOW_VERSION=3.0.6

# [버전 정의]
ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.9.1
ARG SCALA_BINARY_VERSION=2.12
ARG CONFLUENT_VERSION=7.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

FROM apache/airflow:${AIRFLOW_VERSION}-python3.10 AS builder

# FROM 뒤에 ARG를 다시 선언해야 사용 가능
ARG SPARK_VERSION
ARG ICEBERG_VERSION
ARG SCALA_BINARY_VERSION
ARG CONFLUENT_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /tmp/jars

# JAR 파일 다운로드
RUN set -ex; \
    echo "Downloading all required JAR files into builder stage..."; \
    curl -fSL -o "iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar" && \
    curl -fSL -o "iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o "spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" && \
    curl -fSL -o "kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
        "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" && \
    curl -fSL -o "spark-avro_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" && \
    curl -fSL -o "hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -o "aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ==============================================================================
# STAGE 2: 최종 이미지 (Final Image)
# ==============================================================================
FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         zip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

USER airflow

# 여기서 SPARK_HOME이 정의됨
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark

RUN pip install --no-cache-dir -r /tmp/requirements.txt "apache-airflow-providers-apache-spark==5.1.1"

# [핵심] 빌더 스테이지(/tmp/jars)에 다운로드된 파일을 최종 이미지의 SPARK_HOME/jars로 복사
RUN mkdir -p "$SPARK_HOME/jars"
COPY --from=builder /tmp/jars/ "$SPARK_HOME/jars/"