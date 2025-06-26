# 가장 표준적이고 널리 사용됨
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-memory 4g \
  --conf spark.sql.shuffle.partitions=200 \
  my_job.py