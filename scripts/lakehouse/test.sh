# # # default: --deploy-mode client
docker exec infra-spark-client-1 \
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.my=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my.type=rest \
  --conf spark.sql.catalog.my.uri=http://rest-catalog:8181 \
  --conf spark.sql.catalog.my.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.my.warehouse=s3://warehouse \
  --conf spark.sql.catalog.my.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.my.s3.path-style-access=true \
  --conf spark.sql.catalog.my.s3.region=us-east-1 \
  --conf spark.sql.catalog.my.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.my.s3.secret-access-key=minioadmin \
  --conf spark.executorEnv.AWS_REGION=us-east-1 \
  --conf spark.executorEnv.AWS_ACCESS_KEY_ID=minioadmin \
  --conf spark.executorEnv.AWS_SECRET_ACCESS_KEY=minioadmin \
  --conf spark.driver.extraJavaOptions=-Daws.region=us-east-1 \
  --conf spark.executor.extraJavaOptions=-Daws.region=us-east-1 \
  test/rest_catalog.py

# docker exec infra-spark-client-1 bash -c "
# spark-sql --master spark://spark-master:7077 \
#   -e 'CREATE TABLE my.db.demo (id int);'"
#   # -e 'DROP TABLE IF EXISTS my.db.demo; \
#   #     CREATE TABLE my.db.demo (id int); \
#   #     INSERT INTO my.db.demo VALUES (1); \
#   #     SELECT * FROM my.db.demo;'"


# # # docker exec infra-spark-client-1 spark-submit --master spark://spark-master:7077 \
# # #   --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
# # #   --conf spark.sql.catalog.my_catalog.type=rest \
# # #   --conf spark.sql.catalog.my_catalog.uri=http://rest-catalog:8181 \


# # docker exec infra-spark-client-1 spark-sql --master spark://spark-master:7077 \
# #   --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
# #   --conf spark.sql.catalog.my_catalog.type=rest \
# #   --conf spark.sql.catalog.my_catalog.uri=http://rest-catalog:8181 \
# #   -e "CREATE TABLE my_catalog.db.demo (id int); INSERT INTO my_catalog.db.demo VALUES (1); SELECT * FROM my_catalog.db.demo;"

# #   # -e "SHOW TABLES IN my_catalog.db;"
# # #   # -e "DELETE FROM iceberg_tables WHERE table_name = 'demo' AND table_namespace = 'db';"