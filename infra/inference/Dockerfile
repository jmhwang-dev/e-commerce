# CUDA 12.2 + Ubuntu 22.04 런타임 베이스
FROM nvidia/cuda:12.2.2-runtime-ubuntu22.04

# 시스템 패키지, Python 3.10 설치 (재시도 로직 포함)
RUN set -eux; \
    # NVIDIA 리포지터리 미러 동기화 문제 대응 - 최대 3회 재시도
    for i in {1..3}; do \
        apt update && break || { \
            echo "Attempt $i failed, waiting 30s..."; \
            sleep 3; \
        } \
    done && \
    DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
        python3.10-minimal python3-pip ca-certificates && \
    ln -sf /usr/bin/python3.10 /usr/local/bin/python && \
    ln -sf /usr/bin/pip3 /usr/local/bin/pip && \
    rm -rf /var/lib/apt/lists/*

# Python 패키지 의존성 설치
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir -r /tmp/requirements.txt

ARG USERNAME=appuser
ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} ${USERNAME} && \
    useradd -m -u ${UID} -g ${GID} -s /bin/bash ${USERNAME}

RUN mkdir -p /app && \
    chown ${UID}:${GID} /app && \
    chmod u+rwx /app

# Switch to non-root user
USER ${UID}:${GID}

WORKDIR /mnt/app