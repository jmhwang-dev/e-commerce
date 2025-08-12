from service.io.minio import *
from service.io.iceberg import *
from pyiceberg.catalog import load_catalog

print('zzxcvzxcvzxcvzxcv----------------')

DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.review"
DST_TABLE_NAME = "raw"
# spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

catalog = load_catalog(
    "REST",
    **{
        "type": "REST",
        "uri": "http://localhost:8181",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.use-ssl": "false",
        "warehouse": S3_URI
    }
)

table = catalog.load_table(full_table_name)
print(table)


# data_dict = {
#     "review_id": ["f7c4243c7fe1938f181bec41a392bdeb"],
#     "comment_type": ["comment"],
#     "por2eng": ["congratulations lannister stores i loved buying online safe and practical congratulations to everyone happy easter"],
#     "sentimentality": ["positive"]
# }

# tables_to_delete = [
#     (NAMESPCE, TABLE_NAME, f"s3://warehouse-dev/silver/"),
# ]
# delete_table(tables_to_delete)
# table_data = get_table_data(data_dict)
# insert(table_data)