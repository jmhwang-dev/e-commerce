from service.utils.spark import get_spark_session
from service.utils.iceberg import initialize_namespace
from service.producer.bronze import BronzeTopic

SPARK_SESSION = get_spark_session("deduplicate")
BRONZE_NAMESPACE = 'bronze'
SILVER_NAMESPACE = 'silver'

def deduplicates_bronze(table_name, ):
    src_table_identifier = f'{BRONZE_NAMESPACE}.{table_name}'
    dst_table_identifier = f'{SILVER_NAMESPACE}.{table_name}'

    src_stream_df = SPARK_SESSION.read.table(src_table_identifier)
    processed_df = src_stream_df.drop_duplicates()

    if not SPARK_SESSION.catalog.tableExists(dst_table_identifier):
        processed_df.writeTo(dst_table_identifier).create()
        return

    dst_df = SPARK_SESSION.read.table(dst_table_identifier)
    if processed_df.count() == dst_df.count():
        return

    dst_df.writeTo(dst_table_identifier).overwrite()

if __name__ == "__main__":
    initialize_namespace(SPARK_SESSION, 'silver', is_drop=False)

    src_table_names = [
        BronzeTopic.SELLER,
        BronzeTopic.CUSTOMER,
        BronzeTopic.GEOLOCATION
        ]
    
    for table_name in src_table_names:
        deduplicates_bronze(table_name)
    
    SPARK_SESSION.stop()