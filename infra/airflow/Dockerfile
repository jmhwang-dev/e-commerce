# ==============================================================================
# STAGE 1: 빌더 스테이지 (Builder Stage)
# - JAR 파일 다운로드와 같은 빌드에만 필요한 작업을 수행합니다.
# - 최종 이미지 크기를 줄이기 위해 사용됩니다.
# ==============================================================================
# latest version on 2025-09-10
ARG AIRFLOW_VERSION=3.0.6

# [수정] Spark 이미지와 모든 ARG 버전을 완전히 동일하게 맞춥니다.
ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.9.1
ARG SCALA_BINARY_VERSION=2.12
ARG CONFLUENT_VERSION=7.6.1

FROM apache/airflow:${AIRFLOW_VERSION}-python3.10 AS builder

ARG SPARK_VERSION
ARG ICEBERG_VERSION
ARG SCALA_BINARY_VERSION
ARG CONFLUENT_VERSION

# 빌드에 필요한 도구(curl) 설치
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /tmp/jars

# JAR 파일들을 빌더 스테이지의 특정 폴더로 다운로드합니다.
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
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar";

# ==============================================================================
# STAGE 2: 최종 이미지 (Final Image)
# - 실제 운영 환경에 필요한 것들만 포함하는 가벼운 이미지를 만듭니다.
# ==============================================================================
FROM apache/airflow:${AIRFLOW_VERSION}-python3.10

# --- root 유저로 실행할 작업 ---
USER root

# 시스템 의존성 설치 (curl과 같은 빌드 도구는 제외)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         zip \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# requirements.txt를 먼저 복사하여 종속성 캐싱을 활용
COPY requirements.txt /tmp/requirements.txt

# --- airflow 유저로 전환 후 실행할 작업 ---
USER airflow

ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark

# Python 패키지를 먼저 설치합니다. requirements.txt가 변경되지 않으면 이 레이어는 캐시됩니다.
RUN pip install --no-cache-dir -r /tmp/requirements.txt "apache-airflow-providers-apache-spark==5.1.1"

# 빌더 스테이지에서 다운로드한 JAR 파일들을 최종 이미지로 복사
RUN mkdir -p "$SPARK_HOME/jars"
COPY --from=builder /tmp/jars/ "$SPARK_HOME/jars/"