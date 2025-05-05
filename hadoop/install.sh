#!/bin/bash
set -e

HADOOP_VERSION=3.4.1
INSTALL_DIR=/opt
DOWNLOADS_DIR="./downloads/hadoop-${HADOOP_VERSION}"
HADOOP_TAR="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_TAR_ASC="${HADOOP_TAR}.asc"
HADOOP_TAR_SHA512="${HADOOP_TAR}.sha512"
HADOOP_URL="https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}"
KEYS_URL="https://dlcdn.apache.org/hadoop/common/KEYS"

# 다운로드 디렉토리 생성
mkdir -p "$DOWNLOADS_DIR"

# 파일 다운로드 (이미 있는 경우 건너뛰기)
for FILE in "$HADOOP_TAR" "$HADOOP_TAR_ASC" "$HADOOP_TAR_SHA512"; do
    if [ ! -f "$DOWNLOADS_DIR/$FILE" ]; then
        echo "[INFO] Downloading $FILE..."
        wget -q -P "$DOWNLOADS_DIR" "$HADOOP_URL/$FILE"
    fi
done

# KEYS 파일 다운로드
if [ ! -f "$DOWNLOADS_DIR/KEYS" ]; then
    echo "[INFO] Downloading KEYS..."
    wget -q -P "$DOWNLOADS_DIR" "$KEYS_URL"
fi

# SHA512 체크섬 검증
echo "[INFO] Verifying SHA512 checksum..."
(cd "$DOWNLOADS_DIR" && sha512sum -c "$HADOOP_TAR_SHA512") > /dev/null
echo "[INFO] SHA512 checksum verified successfully."

# GPG 공개키 가져오기 및 서명 검증
echo "[INFO] Importing GPG public key..."
gpg --import "$DOWNLOADS_DIR/KEYS"

echo "[INFO] Verifying GPG signature..."
gpg --verify "$DOWNLOADS_DIR/$HADOOP_TAR_ASC" "$DOWNLOADS_DIR/$HADOOP_TAR"
echo "[INFO] GPG signature verified successfully."

# Hadoop 압축 해제
echo "[INFO] Extracting Hadoop..."
sudo tar -xzf "$DOWNLOADS_DIR/$HADOOP_TAR" -C "$INSTALL_DIR"

# Java 설치
echo "[INFO] Installing Java..."
sudo apt-get update -qq
sudo apt-get install -y openjdk-8-jdk