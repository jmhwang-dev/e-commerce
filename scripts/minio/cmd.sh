# install
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/

# connect
mc alias set localminio http://localhost:9000 minioadmin minioadmin

# backup
mc mirror localminio/olist-data /tmp/olist-backup

# check
ls /tmp/olist-backup