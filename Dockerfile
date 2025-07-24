# ───────────────────────────────────────────────────────────────────────────────
# 1. GPU 지원 CUDA 런타임 베이스
FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04


# ───────────────────────────────────────────────────────────────────────────────
# 2. 시스템 의존성 & Python3 설치
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python3.11 python3.11-venv python3-pip git-lfs curl && \
    rm -rf /var/lib/apt/lists/*

# 기본 python 명령을 python3.11로 맞춰 줍니다
RUN ln -s /usr/bin/python3.11 /usr/local/bin/python && \
    ln -s /usr/bin/pip3 /usr/local/bin/pip

# ───────────────────────────────────────────────────────────────────────────────
# 3. Hugging Face 모델 미리 다운로드
#    (컨테이너 빌드 시점에 git-lfs login 필요 없도록, 토큰을 --build-arg로 전달하거나
#     이미 로컬에 내려받은 모델 디렉터리를 복사해 둘 수도 있습니다.)
ARG HF_TOKEN
RUN git lfs install && \
    mkdir -p /app/models && cd /app/models && \
    git clone https://${HF_TOKEN}@huggingface.co/Unbabel/TowerInstruct-7B-v0.2 TowerInstruct-7B-v0.2

# ───────────────────────────────────────────────────────────────────────────────
# 4. 애플리케이션 코드 복사 & 의존성 설치
WORKDIR /app
COPY src/ ./src/
COPY requirements.txt .

# requirements.txt에:
# transformers==4.51.3
# torch==2.6.0
# accelerate==1.6.0
# confluent-kafka>=2.11.0,<3.0.0
RUN pip install --no-cache-dir -r requirements.txt

# ───────────────────────────────────────────────────────────────────────────────
# 5. 환경 변수 설정
ENV TRANSFORMERS_CACHE=/app/models
ENV HF_HOME=/app/models

# ───────────────────────────────────────────────────────────────────────────────
# 6. FastAPI 앱 실행
#    GPU가 자동으로 할당되도록 torch_dtype, device_map을 컨슈머 코드에서 지정하세요.
CMD ["uvicorn", "consumer.main:app", "--host", "0.0.0.0", "--port", "8000"]
