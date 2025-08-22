from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from service.producer.silver import PaymentSilverProducer
from service.utils.spark import start_console_stream
from functools import reduce

def float2int(df: DataFrame, col_names: List) -> DataFrame:
    for col_name in col_names:
        df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
    return df

def publish_payment_dlg(decoded_stream_df: DataFrame) -> DataFrame:
    # notnull_rows_df 생성 (기존처럼, dropna는 스트리밍 지원)
    notnull_rows_df = decoded_stream_df.dropna(how='any')  # 또는 subset으로 특정 컬럼 지정

    # null 행 조건: 모든 컬럼 중 하나라도 null (스트리밍 filter 지원)
    null_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in decoded_stream_df.columns])
    null_row_df = decoded_stream_df.filter(null_condition)

    # DLQ publish: 스트리밍으로 직접 publish (exceptAll 피함)
    PaymentSilverProducer.publish_dlq(null_row_df)

    # notnull 행 반환 (후속 처리용)
    return notnull_rows_df