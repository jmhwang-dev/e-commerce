
from init import *
from iceberg.io import *

data_dict = {
    "review_id": ["f7c4243c7fe1938f181bec41a392bdeb"],
    "comment_type": ["comment"],
    "por2eng": ["congratulations lannister stores i loved buying online safe and practical congratulations to everyone happy easter"],
    "sentimentality": ["positive"]
}

tables_to_delete = [
    (NAMESPCE, TABLE_NAME, f"s3://warehouse-dev/silver/"),
]
delete_table(tables_to_delete)
table_data = get_table_data(data_dict)
insert(table_data)