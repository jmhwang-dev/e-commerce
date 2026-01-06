#!/bin/bash

MODEL1="Unbabel/TowerInstruct-7B-v0.2"
MODEL2="j-hartmann/sentiment-roberta-large-english-3-classes"
CPU_RATIO=75

# 워커 수 계산 (CPU 코어 수 * CPU_RATIO / 100, 최소 1)
workers=$(( ($(nproc) * CPU_RATIO + 99) / 100 ))
[ $workers -lt 1 ] && workers=1

# 모델 다운로드
huggingface-cli download "$MODEL1" --local-dir downloads/models/translate --max-workers "$workers" || {
    echo "$MODEL1 download failed, continuing"
}
huggingface-cli download "$MODEL2" --local-dir downloads/models/sentiment --max-workers "$workers" || {
    echo "$MODEL2 download failed, continuing"
}