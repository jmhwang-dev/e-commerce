# default: --deploy-mode client
docker exec spark-spark-client-1 /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  jobs/load_olist_customers.py
