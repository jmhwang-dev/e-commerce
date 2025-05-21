# Hadoop compilation requires Java 8. Java 11 is not supported for compilation (as of May 9, 2025).
export JAVA_PACKAGE="openjdk-8-jdk"
export JDK_DIR_NAME_PATTERN="java-8-openjdk*"

# download
export DOWNLOAD_DIR='/tmp'
export HADOOP_VERSION="3.4.1"
export HADOOP_DIST_NAME="hadoop-${HADOOP_VERSION}"
export HADOOP_BASE_URL="https://dlcdn.apache.org/hadoop/common"

# install
export HADOOP_INSTALL_DIR='/opt'
export HADOOP_HOME="${HADOOP_INSTALL_DIR}/${HADOOP_DIST_NAME}"
export HADOOP_CONF_DIR="${PWD}/hadoop/conf"