FROM apachesuperset.docker.scarf.sh/apache/superset:5.0.0

USER root

RUN apt-get update && apt-get install -y \
    build-essential \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    && rm -rf /var/lib/apt/lists/*

RUN uv pip install .[hive]
USER superset