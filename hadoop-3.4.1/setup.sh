#!/bin/bash
HADOOP_INSTALL_DIR="/opt/hadoop-3.4.1"

OS_TYPE=$(uname)

if [[ "$OS_TYPE" == "Darwin" ]]; then
    PROFILE_FILE="$HOME/.zshrc"

elif [[ "$OS_TYPE" == "Linux" ]]; then
    PROFILE_FILE="$HOME/.bashrc"
else
  echo "Unsupported operating system: $OS_TYPE"
  exit 1
fi

# Environment variable settings
HADOOP_ENV="
# >>> Hadoop Environment >>>
export HADOOP_INSTALL_DIR=$HADOOP_INSTALL_DIR
export PATH=\$PATH:\$HADOOP_INSTALL_DIR/bin:\$HADOOP_INSTALL_DIR/sbin
# <<< Hadoop Environment <<<
"

# Prevent duplication
if grep -q "HADOOP_INSTALL_DIR" "$PROFILE_FILE"; then
  echo "HADOOP_INSTALL_DIR is already set in $PROFILE_FILE."
else
  echo "$HADOOP_ENV" >> "$PROFILE_FILE"
  echo "Added HADOOP_INSTALL_DIR settings to $PROFILE_FILE."
  
  # Apply the changes immediately
  source "$PROFILE_FILE"
  echo "Environment variables applied."
fi
