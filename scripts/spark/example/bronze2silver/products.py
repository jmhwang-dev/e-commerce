from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

def get_unique_categories(products: DataFrame, categories: DataFrame) -> DataFrame:
    """
    제품 데이터에서 실제 사용되는 카테고리와 카테고리 번역 정보를 left join하여 반환.
    """
    real_product_categories = (
        products
        .select('product_category_name')
        .dropDuplicates()
        .filter(~col('product_category_name').isNull())
    )
    unique_categories = real_product_categories.join(categories, on='product_category_name', how='left')
    return unique_categories

def fill_missing_category_names_manually(categories: DataFrame) -> DataFrame:
    """
    카테고리 영문명이 누락된 경우, 하드코딩 매핑값으로 채움.
    """
    return categories.withColumn(
        "product_category_name_english",
        when(col("product_category_name") == "pc_gamer", "gaming_pc")
        .when(col("product_category_name") == "portateis_cozinha_e_preparadores_de_alimentos", "portable_kitchen_and_food_preparators")
        .otherwise(col('product_category_name_english'))
    )

def make_column_rename_dict(columns: list) -> dict:
    """
    컬럼명 변경 규칙을 dict로 반환.
    - product_id는 그대로, 
    - product_category_name_english는 'category'로,
    - 그 외는 첫 prefix 제외 + 오타(lenght → length) 보정
    """
    rename_dict = {}
    for c in columns:
        if c == "product_id":
            rename_dict[c] = c
        elif c == "product_category_name_english":
            rename_dict[c] = "category"
        else:
            parts = c.split("_")[1:]
            # 오타 보정
            parts = ["length" if p == "lenght" else p for p in parts]
            new_c = "_".join(parts)
            rename_dict[c] = new_c
    return rename_dict

def rename_columns(df: DataFrame, rename_dict: dict) -> DataFrame:
    """
    컬럼명을 rename_dict 기준으로 일괄 변경
    """
    new_columns = [rename_dict.get(c, c) for c in df.columns]
    return df.toDF(*new_columns)

if __name__ == "__main__":
    spark = SparkSession.builder.appName('products_spec').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 데이터 로드
    PRODUCTS_TABLE_NAME = "warehousedev.silver.dedup.olist_products_dataset"
    ENG_CATEGORIES_TABLE_NAME = "warehousedev.silver.dedup.product_category_name_translation"

    products = spark.table(PRODUCTS_TABLE_NAME)
    eng_categories = spark.table(ENG_CATEGORIES_TABLE_NAME)

    # Step 1. 카테고리 매핑
    unique_categories = get_unique_categories(products, eng_categories)

    # Step 2. 누락된 카테고리 영문명 보정
    null_unique_categories = unique_categories.filter(col("product_category_name_english").isNull())
    if null_unique_categories.count() != 0:
        # 누락 값 보정
        unique_categories = fill_missing_category_names_manually(unique_categories)
        print("[INFO] 누락된 카테고리 영문명을 보정하였습니다:")
        unique_categories.filter(
            col("product_category_name_english").isin(
                ["gaming_pc", "portable_kitchen_and_food_preparators"]
            )
        ).show(truncate=False)

    # Step 3. product 데이터와 조인
    product_spec = products.join(unique_categories, on='product_category_name', how='left').drop("product_category_name")

    # Step 4. 컬럼명 표준화
    rename_dict = make_column_rename_dict(product_spec.columns)
    product_spec = rename_columns(product_spec, rename_dict)

    # Step 5. 테이블 저장
    DST_QUALIFIED_NAMESPACE = "warehousedev.silver.products"
    DST_TABLE_NAME = "products_spec"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    writer = product_spec.writeTo(full_table_name) \
        .tableProperty("comment", "Replace Portuguese to English for `category name`.")

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    product_spec.show(n=5)
    
    spark.stop()