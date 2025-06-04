# connect
mc alias set localminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# backup
mc mirror localminio/olist-data /tmp/olist-backup

# check
ls /tmp/olist-backup
