# 안됨: bundle-2.31.54 는 aws sdk v2. hadoop-aws-3.3.4는 v1 이기 때문
# curl -fSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
#         https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar


curl -fSL https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar -o /opt/spark/jars/iceberg-aws-bundle-1.9.1.jar
curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar
curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar
# curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.262/aws-java-sdk-s3-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-s3-1.12.262.jar
curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar



/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar,/opt/spark/jars/iceberg-aws-bundle-1.9.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  load_olist_customers.py


/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.1.jar,/opt/spark/jars/iceberg-aws-bundle-1.9.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
  iceberge.py