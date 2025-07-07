ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.9.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12
ARG SCALA_BINARY_VERSION=2.12
ARG JAVA_VERSION=17
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

FROM apache/spark:${SPARK_VERSION}-scala${SCALA_BINARY_VERSION}-java${JAVA_VERSION}-python3-ubuntu

ARG ICEBERG_VERSION
ARG ICEBERG_SPARK_RUNTIME_VERSION
ARG SPARK_VERSION
ARG SCALA_BINARY_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION

USER root
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
WORKDIR $SPARK_HOME/work-dir

# Copy requirements.txt first to leverage Docker layer cache
COPY requirements.txt /tmp/requirements.txt

# Download all JARs and install pip packages in parallel
RUN ( \
    curl -fSL -o $SPARK_HOME/jars/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar & \
    curl -fSL -o $SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar & \
    curl -fSL -o $SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar & \
    curl -fSL -o $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar & \
    pip3 install --no-cache-dir -r /tmp/requirements.txt \
    && wait )
