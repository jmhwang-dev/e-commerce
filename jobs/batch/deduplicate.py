from service.utils.spark import get_spark_session
from service.producer.bronze import BronzeTopic
from service.utils.logger import *

SPARK_SESSION = get_spark_session("Silver")
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
    src_table_names = [
        BronzeTopic.PRODUCT,
        BronzeTopic.SELLER,
        BronzeTopic.CUSTOMER,
        BronzeTopic.GEOLOCATION
        ]
    
    for table_name in src_table_names:
        deduplicates_bronze(table_name)
    
    SPARK_SESSION.stop()