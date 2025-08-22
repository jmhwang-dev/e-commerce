from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, cast
from pyspark.sql.types import IntegerType, FloatType

def float2int(df: DataFrame, col_names: List) -> DataFrame:
    for col_name in col_names:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
    return df