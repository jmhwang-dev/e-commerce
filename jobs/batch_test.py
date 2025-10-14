
from typing import List

from service.batch.silver import *
from schema.silver import *

from service.utils.spark import get_spark_session
from service.utils.iceberg import initialize_namespace

SPARK_SESSION = get_spark_session("Silver")

if __name__ == "__main__":
    spark = get_spark_session("Silver Batch Job", dev=True)

    job_instance: SilverBatchJob = OrderTimeline(spark)
    i = 0
    end = 3
    while i < end:
        job_instance.generate()
        job_instance.update_table()
        # job_instance.update_watermark()
        i += 1

    SPARK_SESSION.stop()