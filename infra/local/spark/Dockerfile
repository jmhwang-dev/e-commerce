FROM apache/spark:3.5.6-scala2.12-java17-ubuntu

USER root
# latest aws sdk for hadoop 3.3.4 1.12.787
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip curl && \
    # 병렬 다운로드 및 설치
    # curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    #     https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar & \
    curl -fsSL -o /opt/spark/jars/bundle-2.31.54.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.54/bundle-2.31.54.jar & \
    curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar \
        https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar  & \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar & \
    curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-1.9.1.jar \
        https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar  & \
    pip3 install --no-cache-dir ipykernel pandas matplotlib seaborn pyspark==3.5.6 & \
    wait && \
    # MinIO client
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -fsSL https://dl.min.io/client/mc/release/linux-arm64/mc -o /usr/bin/mc; \
    else \
        curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/bin/mc; \
    fi && \
    chmod +x /usr/bin/mc && \
    # 정리
    rm -rf /var/lib/apt/lists/*

ENV PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

WORKDIR /opt/spark/work-dir
