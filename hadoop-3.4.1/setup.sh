#!/bin/bash

HADOOP_CONF_DIR=${PWD}/hadoop-3.4.1/etc/hadoop/hadoop-env.sh

# Source the Hadoop environment configuration
. $HADOOP_CONF_DIR

# Check the operating system type and set appropriate variables
if [[ "$HADOOP_OS_TYPE" == "Darwin" ]]; then
    PROFILE_FILE="$HOME/.zshrc"
    JAVA_HOME=/usr/local/opt/openjdk@8

elif [[ "$HADOOP_OS_TYPE" == "Linux" ]]; then
    PROFILE_FILE="$HOME/.bashrc"
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre

else
    echo "Unsupported operating system: $HADOOP_OS_TYPE"
    exit 1
fi

# Environment variable settings
HADOOP_ENV="
# >>> Hadoop Environment >>>
export PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}
export JAVA_HOME=${JAVA_HOME}
# <<< Hadoop Environment <<<
"

# Prevent duplication of the HADOOP_ENV settings in the profile file
if grep -q "HADOOP_ENV" "$PROFILE_FILE"; then
    echo "HADOOP_ENV is already set in $PROFILE_FILE."
else
    # Append the environment settings to the profile file
    echo "$HADOOP_ENV" >> "$PROFILE_FILE"
    echo "Added HADOOP_ENV settings to $PROFILE_FILE."

    # Apply the changes immediately
    source "$PROFILE_FILE"
    echo "Environment variables applied."
fi