source ./hadoop/install/config.sh
source ./hadoop/install/java.sh
source ./hadoop/install/hadoop.sh
source ./hadoop/install/profile.sh

sudo mkdir -p /mnt/hadoop/dfs/name
sudo mkdir -p /mnt/hadoop/dfs/data
sudo chown -R jmhwang:jmhwang /mnt/hadoop