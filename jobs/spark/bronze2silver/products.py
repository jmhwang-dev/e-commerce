from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, when

def get_unique_categories(products:DataFrame, categories:DataFrame) -> DataFrame:
    real_product_categories = \
        products.select('product_category_name') \
            .dropDuplicates() \
            .filter(~col('product_category_name').isNull())
    
    unique_categories = real_product_categories.join(categories, on='product_category_name', how='left')

    return unique_categories

def fill_missing_category_names_manually(unique_categories: DataFrame) -> DataFrame:
    filled_categories = unique_categories.withColumn(
        "product_category_name_english",
        when( col("product_category_name") == "pc_gamer", "gaming_pc")
        .when( col("product_category_name") == "portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_and_food_preparators")
        .otherwise(col('product_category_name_english'))
    )

    filled_categories \
        .filter(col('product_category_name_english').isin(["gaming_pc", "portable_kitchen_and_food_preparators"])) \
            .show(truncate=False)
    
    return filled_categories

def modify_column_name(eng_products: DataFrame) -> DataFrame:
    for col_name in eng_products.columns:
        if col_name == "product_id":
            continue
        if col_name == "product_category_name_english":
            new_col_name = 'category'
        else:
            new_col_words = col_name.split("_")[1:]
            new_col_words = list(map(lambda x: "length" if x == 'lenght' else x, new_col_words))            
            new_col_name = '_'.join(new_col_words)
        eng_products = eng_products.withColumnRenamed(col_name, new_col_name)
    return eng_products

if __name__=="__main__":
    spark = SparkSession.builder.appName('translate_product_categories').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # "ERROR"도 가능

    PRODUCTS_TABLE_NAME = f"warehouse_dev.silver.dedup.olist_products_dataset"
    products = spark.table(PRODUCTS_TABLE_NAME)

    ENG_CATEGORIES_TABLE_NAME = f"warehouse_dev.silver.dedup.product_category_name_translation"
    eng_categories = spark.table(ENG_CATEGORIES_TABLE_NAME)

    unique_categories = get_unique_categories(products, eng_categories)
    null_unique_categories = unique_categories.filter(col("product_category_name_english").isNull())

    if null_unique_categories.count() != 0:
        unique_categories = fill_missing_category_names_manually(unique_categories)

    product_spec = products.join(unique_categories, on='product_category_name', how='left').drop("product_category_name")
    product_spec = modify_column_name(product_spec)

    DST_QUALIFIED_NAMESPACE = f"warehouse_dev.silver.products"
    DST_TABLE_NAME = "product_spec"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")    
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    if not spark.catalog.tableExists(full_table_name):
        product_spec.writeTo(full_table_name).create()
    else:
        product_spec.writeTo(full_table_name).overwritePartitions()