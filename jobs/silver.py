from service.utils.spark import get_spark_session
from service.utils.iceberg.spark import get_snapshot_id_by_time_boundary, get_snapshot_df, TimeBoundary, get_next_start_id
from service.pipeline.silver import *

import time
if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark_session = get_spark_session("SilverJob")

    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.clean_namespace}")
    spark_session.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.error_namespace}")

    i = 0
    snapshot_df = get_snapshot_df(spark_session, PaymentSilverJob.src_table_identifier)
    start_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.EARLIEST)
    end_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.LATEST)

    while i < 2:
        
        incremental_df = spark_session.read \
            .format("iceberg") \
            .option("start-snapshot-id", start_snapshot_id) \
            .option("end-snapshot-id", end_snapshot_id) \
            .load(PaymentSilverJob.src_table_identifier)
        
        PaymentSilverJob.common_etl(spark_session, incremental_df)

        i += 1
        time.sleep(30)
        snapshot_df = get_snapshot_df(spark_session, PaymentSilverJob.src_table_identifier)
        start_snapshot_id = get_next_start_id(snapshot_df, end_snapshot_id)
        end_snapshot_id = get_snapshot_id_by_time_boundary(snapshot_df, TimeBoundary.LATEST)
        
    spark_session.stop()