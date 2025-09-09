from typing import List

from service.utils.spark import get_spark_session
from service.batch.silver import *
from schema.silver import *

if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Batch Job")
    silver_batch_job_list: List[SilverBatchJob] = [
        OrderProduct,
    ]

    i = 0
    end = 2
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverBatchJob.dst_namesapce}")

    while i < end:
        try:
            for job_class in silver_batch_job_list:
                job_instance: SilverBatchJob = job_class(spark)
                job_instance.generate()
                job_instance.update_table()
                job_instance.update_watermark()                
            i += 1
        finally:
            pass
    spark.stop()