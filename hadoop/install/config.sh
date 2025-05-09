# Hadoop compilation requires Java 8. Java 11 is not supported for compilation (as of May 9, 2025).
export JAVA_PACKAGE="openjdk-8-jdk"
export JDK_DIR_NAME_PATTERN="java-8-openjdk*"

export HADOOP_VERSION="3.4.1"
export HADOOP_DIST_NAME="hadoop-${HADOOP_VERSION}"
export HADOOP_HOME="/opt/${HADOOP_DIST_NAME}"
export HADOOP_CONF_DIR="${PWD}/hadoop/conf"