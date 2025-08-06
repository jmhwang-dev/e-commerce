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

# Create and set permissions for /mnt/logs
RUN mkdir -p /app && \
    chown ${UID}:${GID} /app && \
    chmod u+rwx /app

USER ${UID}:${GID}


# Switch to non-root user
WORKDIR /app

# Hugging Face 고속 다운로드 활성화
# ENV HF_HUB_ENABLE_HF_TRANSFER=1

# # 모델 프리패치 (CPU 코어 사용률 설정)
# ARG MODEL1="Unbabel/TowerInstruct-7B-v0.2"
# ARG MODEL2="j-hartmann/sentiment-roberta-large-english-3-classes"
# ARG CPU_RATIO=75

# # CPU 사용률에 따른 다운로드 워커 수 계산 (최대 3회 재시도)
# RUN set -e; \
#     workers=$(( ( $(nproc) * ${CPU_RATIO} + 99 ) / 100 )); \
#     workers=$(( workers < 1 ? 1 : workers )); \
#     echo "Using $workers download workers (${CPU_RATIO}% of $(nproc) cores)"; \
#     for i in {1..3}; do \
#         hf download "$MODEL2" \
#             --local-dir /models/sentiment \
#             --max-workers "$workers" && break || sleep 5; \
#     done && \
#     for i in {1..3}; do \
#         hf download "$MODEL1" \
#             --local-dir /models/tower \
#             --max-workers "$workers" && break || sleep 5; \
#     done

# 작업 디렉터리 설정
# WORKDIR /app