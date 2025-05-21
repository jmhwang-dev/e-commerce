echo "[INFO] Installing Java..."
sudo apt-get install -qy ${JAVA_PACKAGE}

export JAVA_HOME=$(find /usr/lib/jvm -type d -name ${JDK_DIR_NAME_PATTERN} | head -n 1)