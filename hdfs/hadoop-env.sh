OS=$(uname)

if [ "$OS" = "Darwin" ]; then
    # brew install openjdk@8
    export JAVA_HOME="/usr/local/opt/openjdk@8"
elif [ "$OS" = "Linux" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
else
    echo "JAVA_HOME is not set: Unknown operating system"
fi

export JAVA_HOME=$JAVA_HOME
export HADOOP_HOME=/opt/hadoop-3.4.1
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin