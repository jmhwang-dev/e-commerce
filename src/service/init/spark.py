from pyspark.sql import SparkSession

DESERIALIZE_OPTIONS = {
    "mode": "PERMISSIVE",
    "columnNameOfCorruptRecord": "_corrupt_record"
}

def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark