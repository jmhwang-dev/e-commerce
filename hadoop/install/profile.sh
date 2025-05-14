#!/bin/bash

# 하둡 환경 변수 설정 파일 경로
HADOOP_PROFILE="/etc/profile.d/hadoop.sh"

# 하둡 환경 변수 및 경로 추가 함수 정의
HADOOP_EXPORT_BLOCK=$(cat <<EOF
# >>> HADOOP ENV START >>>
export HADOOP_HOME=${HADOOP_HOME}
export JAVA_HOME=${JAVA_HOME}

# PATH에 이미 추가되어 있는지 확인
add_to_path() {
    # \$1: 추가하려는 경로
    if [[ ":\$PATH:" != *":\$1:"* ]]; then
        # 경로가 없으면 앞에 추가
        export PATH="\$1:\$PATH"
        echo "[INFO] Added \$1 to PATH"
    else
        # 이미 경로가 존재하면 아무 작업도 하지 않음
        echo "[INFO] \$1 is already in PATH, skipping."
    fi
}

# add_to_path "\$JAVA_HOME/bin"
add_to_path "\$HADOOP_HOME/bin"
add_to_path "\$HADOOP_HOME/sbin"
# <<< HADOOP ENV END <<<
EOF
)

# HADOOP_PROFILE이 존재하지 않으면 생성
if [ ! -f "$HADOOP_PROFILE" ]; then
    echo "[INFO] Creating $HADOOP_PROFILE..."
    echo "$HADOOP_EXPORT_BLOCK" | sudo tee "$HADOOP_PROFILE" > /dev/null
else
    echo "[INFO] $HADOOP_PROFILE already exists. Skipping."
fi

# ~/.bash_profile 파일이 없으면 생성
if [ ! -f "$HOME/.bash_profile" ]; then
    echo "[INFO] ~/.bash_profile not found. Creating..."
    touch ~/.bash_profile
fi

# ~/.bash_profile 경로
BASH_PROFILE="$HOME/.bash_profile"
# 환경 변수 추가 함수 정의
add_var_to_bash_profile() {
    local var_name="$1"    # 변수 이름 (예: HADOOP_HOME)
    local var_value="$2"   # 변수 값 (예: $HADOOP_HOME)

    # 변수 값이 설정되어 있는 경우
    if [ -n "$var_value" ]; then
        # ~/.bash_profile에 해당 변수가 이미 존재하는지 체크
        if ! grep -q "$var_name" "$BASH_PROFILE"; then
            echo "[INFO] Adding $var_name to $BASH_PROFILE..."
            echo "export $var_name=$var_value" >> "$BASH_PROFILE"
        else
            echo "[INFO] $var_name already exists in $BASH_PROFILE. Skipping."
        fi
    else
        echo "[INFO] $var_name is not set. Skipping addition."
    fi
}

# 각 환경 변수를 ~/.bash_profile에 추가
add_var_to_bash_profile "HADOOP_CONF_DIR" "$HADOOP_CONF_DIR"

# start-dfs.sh 실행을 위한 추가 config
# ssh를 통한 비-인터랙티브(non-interactive) 세션에서 환경변수를 로드하기 위함
echo JAVA_HOME=${JAVA_HOME} > ~/.ssh/environment
echo HADOOP_HOME=${HADOOP_HOME} >> ~/.ssh/environment
echo HADOOP_CONF_DIR=$HADOOP_CONF_DIR >> ~/.ssh/environment

echo "[DONE] Hadoop environment variables configured."