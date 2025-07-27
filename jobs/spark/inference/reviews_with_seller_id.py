from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName('tmp_inference').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    inference_table_name = "warehouse_dev.silver.inference.reviews"
    order_items_table_name = "warehouse_dev.silver.dedup.olist_order_items_dataset"
    order_reviews_table_name = "warehouse_dev.silver.dedup.olist_order_reviews_dataset"

    infer_df = spark.read.table(inference_table_name)
    order_items_df = spark.read.table(order_items_table_name)
    order_reviews_df = spark.read.table(order_reviews_table_name)

    # seller_id 062ce95fa2ad4dfaedfc79260130565f
    unique_seller_by_order = order_items_df.select("seller_id", 'order_id').distinct()
    unique_review_by_order = order_reviews_df.select("review_id", 'order_id').distinct()
    
    infer_with_ordier_id = infer_df.join(unique_review_by_order, on="review_id", how='left')
    infer_with_seller_id = infer_with_ordier_id.join(unique_seller_by_order, on="order_id", how='left')


    full_table_name = "warehouse_dev.silver.inference.reviews_with_seller_id"
    writer = (
        infer_with_seller_id.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "To show inference results of reviews on superset."
        )
    )

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    spark.stop()
