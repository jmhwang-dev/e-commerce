ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.9.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12
ARG SCALA_BINARY=2.12
ARG JAVA_VERSION=17

FROM apache/spark:${SPARK_VERSION}-scala${SCALA_BINARY}-java${JAVA_VERSION}-python3-ubuntu

ARG ICEBERG_VERSION
ARG ICEBERG_SPARK_RUNTIME_VERSION
ARG SPARK_VERSION
ARG SCALA_BINARY

USER root
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
WORKDIR $SPARK_HOME/work-dir

# Iceberg Spark Runtime JAR
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar \
    $SPARK_HOME/jars/

# Iceberg AWS bundle
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    $SPARK_HOME/jars/

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt
