# 연결
mc alias set localminio http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# 백업
mc mirror localminio/olist-data /tmp/olist-backup

# 확인
ls /tmp/olist-backup
