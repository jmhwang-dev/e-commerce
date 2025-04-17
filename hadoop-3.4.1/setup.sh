#!/bin/bash
HADOOP_DIR="/opt/hadoop-3.4.1"

OS_TYPE=$(uname)

if [[ "$OS_TYPE" == "Darwin" ]]; then
    brew update && brew install openjdk@8
    PROFILE_FILE="$HOME/.zshrc"

elif [[ "$OS_TYPE" == "Linux" ]]; then
    sudo apt update && sudo apt install -y openjdk-8-jdk
    PROFILE_FILE="$HOME/.bashrc"
else
  echo "Unsupported operating system: $OS_TYPE"
  exit 1
fi

# Environment variable settings
HADOOP_ENV="
# >>> Hadoop Environment >>>
export HADOOP_HOME=$HADOOP_DIR
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
# <<< Hadoop Environment <<<
"

# Prevent duplication
if grep -q "HADOOP_HOME" "$PROFILE_FILE"; then
  echo "HADOOP_HOME is already set in $PROFILE_FILE."
else
  echo "$HADOOP_ENV" >> "$PROFILE_FILE"
  echo "Added HADOOP_HOME settings to $PROFILE_FILE."
  
  # Apply the changes immediately
  source "$PROFILE_FILE"
  echo "Environment variables applied."
fi
