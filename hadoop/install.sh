#!/bin/bash
set -e

# 버전 및 경로 변수 설정
HADOOP_VERSION=3.4.1
HADOOP_DIR_NAME="hadoop-${HADOOP_VERSION}"
INSTALL_DIR=/opt
DOWNLOADS_DIR="./downloads/${HADOOP_DIR_NAME}"
HADOOP_TAR="${HADOOP_DIR_NAME}.tar.gz"
HADOOP_TAR_ASC="${HADOOP_TAR}.asc"
HADOOP_TAR_SHA512="${HADOOP_TAR}.sha512"
HADOOP_URL="https://dlcdn.apache.org/hadoop/common/${HADOOP_DIR_NAME}"
KEYS_URL="https://dlcdn.apache.org/hadoop/common/KEYS"
HADOOP_HOME="${INSTALL_DIR}/${HADOOP_DIR_NAME}"

# Hadoop이 이미 설치되어 있는지 확인
if [ -d "$HADOOP_HOME" ]; then
    echo "[INFO] Hadoop ${HADOOP_VERSION} is already installed at ${HADOOP_HOME}. Skipping installation."
else
    # 다운로드 디렉토리 생성
    mkdir -p "$DOWNLOADS_DIR"

    # Hadoop 관련 파일 다운로드
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
    (
        cd "$DOWNLOADS_DIR"
        sha512sum -c "$HADOOP_TAR_SHA512"
    ) > /dev/null
    echo "[INFO] SHA512 checksum verified."

    # GPG 키 가져오기 및 서명 검증
    echo "[INFO] Importing GPG key..."
    gpg --import "$DOWNLOADS_DIR/KEYS"

    echo "[INFO] Verifying GPG signature..."
    gpg --verify "$DOWNLOADS_DIR/$HADOOP_TAR_ASC" "$DOWNLOADS_DIR/$HADOOP_TAR"
    echo "[INFO] GPG signature verified."

    # Hadoop 압축 해제
    echo "[INFO] Extracting Hadoop..."
    sudo tar -xzf "$DOWNLOADS_DIR/$HADOOP_TAR" -C "$INSTALL_DIR"
fi

# Java 설치
echo "[INFO] Installing Java..."
sudo apt-get update -qq
sudo apt-get install -y openjdk-8-jdk

# 아키텍처에 따라 JAVA_HOME 설정
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
elif [ "$ARCH" = "aarch64" ]; then
    JAVA_HOME="/usr/lib/jvm/java-8-openjdk-arm64"
else
    echo "[ERROR] Unknown architecture: $ARCH"
    exit 1
fi

# 환경변수 설정 스크립트 작성
HADOOP_CONF_DIR="$(pwd)/hadoop/conf"

echo "[INFO] Configuring environment variables..."
sudo tee /etc/profile.d/hadoop.sh > /dev/null <<EOF
export HADOOP_HOME=${INSTALL_DIR}/${HADOOP_DIR_NAME}
export JAVA_HOME=${JAVA_HOME}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export PATH=\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$PATH
EOF

echo "[DONE] Hadoop ${HADOOP_VERSION} 설치 및 환경 설정 완료."
source /etc/profile.d/hadoop.sh
# TODO: Resolve issue where terminal session closes after running `hdfs`