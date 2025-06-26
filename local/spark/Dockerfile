FROM apache/spark:latest

RUN curl -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar

RUN curl -o /opt/spark/jars/bundle-2.31.54.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.54/bundle-2.31.54.jar

USER root

# Install MinIO client (mc)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "aarch64" ]; then \
        curl -L https://dl.min.io/client/mc/release/linux-arm64/mc -o /usr/bin/mc; \
    else \
        curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/bin/mc; \
    fi && \
    chmod +x /usr/bin/mc

WORKDIR /opt/spark/work-dir

USER spark