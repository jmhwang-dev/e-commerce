ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.9.1
ARG ICEBERG_SPARK_RUNTIME_VERSION=3.5_2.12
ARG SCALA_BINARY_VERSION=2.12
ARG JAVA_VERSION=17
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG CONFLUENT_VERSION=7.6.1
ARG ABRIS_VERSION=6.4.1
ARG POSTGRES_VERSION=42.7.3

FROM apache/spark:${SPARK_VERSION}-scala${SCALA_BINARY_VERSION}-java${JAVA_VERSION}-python3-ubuntu

ARG SPARK_VERSION
ARG ICEBERG_VERSION
ARG ICEBERG_SPARK_RUNTIME_VERSION
ARG SCALA_BINARY_VERSION
ARG JAVA_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_VERSION
ARG CONFLUENT_VERSION
ARG ABRIS_VERSION
ARG POSTGRES_VERSION

USER root
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
WORKDIR $SPARK_HOME/work-dir

COPY requirements.txt /tmp/requirements.txt

RUN set -e; \
    curl -fSL -o "$SPARK_HOME/jars/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${ICEBERG_SPARK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" & \
    curl -fsSL -o "$SPARK_HOME/jars/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" & \
    curl -fsSL -o "$SPARK_HOME/jars/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" & \
    curl -fsSL -o "$SPARK_HOME/jars/kafka-clients-3.4.1.jar" \
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" \
        "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/common-utils-${CONFLUENT_VERSION}.jar" \
        "https://packages.confluent.io/maven/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/spark-avro_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/abris_${SCALA_BINARY_VERSION}-${ABRIS_VERSION}.jar" \
        "https://search.maven.org/remotecontent?filepath=za/co/absa/abris_${SCALA_BINARY_VERSION}/${ABRIS_VERSION}/abris_${SCALA_BINARY_VERSION}-${ABRIS_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" \
        "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/commons-pool2-2.11.1.jar" \
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/kafka-schema-serializer-7.6.6.jar" \
        "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.6.6/kafka-schema-serializer-7.6.6.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/common-config-7.6.0.jar" \
        "https://packages.confluent.io/maven/io/confluent/common-config/7.6.0/common-config-7.6.0.jar" & \
    curl -fSL -o "$SPARK_HOME/jars/postgresql-${POSTGRES_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" & \
    wait; \
    pip3 install --no-cache-dir -r /tmp/requirements.txt


    # curl -O https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.6.6/kafka-schema-serializer-7.6.6.jar
    # curl -O https://packages.confluent.io/maven/io/confluent/common-config/7.6.0/common-config-7.6.0.jar