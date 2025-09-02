from service.utils.spark import get_spark_session
from service.pipeline.silver import *

import time
if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark_session = get_spark_session("SilverJob")

    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.clean_namespace}")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.error_namespace}")

    watermark_table_identifier = SilverJob.watermark_table
    print(f"Ensuring watermark table '{watermark_table_identifier}' exists...")
    empty_df = spark_session.createDataFrame([], WATERMARK_SCHEMA)
    append_or_create_table(spark_session, empty_df, watermark_table_identifier)

    i = 0

    while i < 2:
        payment_job = PaymentSilverJob(spark_session)
        payment_job.run()
        i += 1

    spark_session.stop()