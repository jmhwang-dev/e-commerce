# Hadoop이 이미 설치되어 있는지 확인
if [ -d "$HADOOP_HOME" ]; then
    echo "[INFO] Hadoop ${HADOOP_DIST_NAME} is already installed at ${HADOOP_HOME}. Skipping installation."
    
else
    # 필수 도구 확인 및 설치
    echo "[INFO] Checking for required tools..."
    sudo apt-get update -qq
    for cmd in wget gpg sha512sum; do
        if ! command -v "$cmd" &> /dev/null; then
            echo "[INFO] $cmd is not installed. Attempting to install..."
            case "$cmd" in
                wget) pkg="wget" ;;
                gpg) pkg="gnupg" ;;
                sha512sum) pkg="coreutils" ;;
            esac
            sudo apt-get install -qy "$pkg"
            if ! command -v "$cmd" &> /dev/null; then
                echo "[ERROR] Failed to install $cmd. Please install it manually."
                exit 1
            fi
        else
            echo "[INFO] $cmd is already installed."
        fi
    done

    # Hadoop 관련 파일 다운로드
    for FILE in "$HADOOP_DIST_TAR" "$HADOOP_DIST_TAR_ASC" "$HADOOP_DIST_TAR_SHA512" "$HADOOP_KEYS"; do
        if [ ! -f "$DOWNLOAD_DIR/$FILE" ]; then
            echo "[INFO] Downloading $FILE..."
            
            if [[ "$FILE" != "$HADOOP_KEYS" ]]; then
                DOWNLOAD_URL="${HADOOP_BASE_URL}/${HADOOP_DIST_NAME}"
            else
                DOWNLOAD_URL="${HADOOP_BASE_URL}"
            fi
            
            wget -q "$DOWNLOAD_URL/$FILE" -P "$DOWNLOAD_DIR"
            if [ $? -ne 0 ]; then
                echo "[ERROR] Failed to download $FILE. Aborting."
                exit 1
            fi
        else
            echo "[INFO] $FILE already exists. Skipping download."
        fi
    done

    # SHA512 체크섬 검증
    echo "[INFO] Verifying SHA512 checksum..."
    if (
        cd "$DOWNLOAD_DIR" && \
        sha512sum -c "$HADOOP_DIST_TAR_SHA512" > /dev/null
    ); then
        echo "[INFO] SHA512 checksum verified."
    else
        echo "[ERROR] SHA512 checksum verification failed. Aborting installation."
        exit 1
    fi

    # GPG 서명 검증
    echo "[INFO] Verifying GPG signature..."

    # GPG 키 가져오기
    if gpg --import "$DOWNLOAD_DIR/$HADOOP_KEYS" > /dev/null 2>&1; then
        echo "[INFO] GPG key imported."
    else
        echo "[ERROR] Failed to import GPG key. Aborting installation."
        exit 1
    fi

    # GPG 서명 검증
    if gpg --verify "$DOWNLOAD_DIR/$HADOOP_DIST_TAR_ASC" "$DOWNLOAD_DIR/$HADOOP_DIST_TAR" > /dev/null 2>&1; then
        echo "[INFO] GPG signature verified."
    else
        echo "[ERROR] GPG signature verification failed. Aborting installation."
        exit 1
    fi

    # Hadoop 압축 해제
    echo "[INFO] Extracting Hadoop..."
    if sudo tar -xzf "$DOWNLOAD_DIR/$HADOOP_DIST_TAR" -C "$HADOOP_INSTALL_DIR"; then
        echo "[DONE] Hadoop ${HADOOP_DIST_NAME} installation completed."
    else
        echo "[ERROR] Failed to extract Hadoop archive."
        exit 1
    fi
fi
