# connect
mc alias set localminio http://localhost:9000 minioadmin minioadmin

# backup
mc mirror localminio/olist-data /tmp/olist-backup

# check
ls /tmp/olist-backup