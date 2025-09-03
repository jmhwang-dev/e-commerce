from service.utils.spark import get_spark_session
from service.pipeline.silver import *

import time
if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    SilverJob.initialize()
    i = 0
    end = 5
    while i < end:
        payment_job = PaymentSilverJob()
        payment_job.run()
        i += 1

    df = SilverJob.spark_session.read.table('warehousedev.bronze.stream_payment')
    df.sort('timestamp').show(n= 100, truncate=False)
    print(df.count())
    df_clean = SilverJob.spark_session.read.table('warehousedev.silver.payment')
    df_error = SilverJob.spark_session.read.table('warehousedev.silver.error.payment')
    print(f"{df_clean.count()} + {df_error.count()}")

    SilverJob.spark_session.stop()
    # spark_session.stop()