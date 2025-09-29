# ./jobs/batch/test_spark.py

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HelloWorld") \
        .getOrCreate()

    print("Spark Session is created successfully!")
    
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)
    
    print(f"DataFrame has {df.count()} rows.")
    df.show()
    
    print("Spark job finished successfully.")
    
    spark.stop()