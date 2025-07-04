FROM apache/spark:3.5.6-scala2.12-java17-ubuntu

USER root
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"
WORKDIR /opt/spark/work-dir

RUN set -e && \
    apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    mkdir -p /opt/spark/jars && \
    \
    curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar && \
    curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-1.9.1.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    \
    pip3 install --no-cache-dir ipykernel pandas matplotlib seaborn pyspark==3.5.6 && \
    \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -fsSL https://dl.min.io/client/mc/release/linux-arm64/mc -o /usr/bin/mc ; \
    else \
        curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/bin/mc ; \
    fi && \
    chmod +x /usr/bin/mc && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
