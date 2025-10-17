from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from typing import Union

from .base import BatchJob
from service.utils.iceberg import write_iceberg
from schema.gold import *
from service.utils.iceberg import initialize_namespace
from service.utils.spark import get_spark_session

class GoldBatchJob(BatchJob):
    src_namespace: str = 'silver'
    dst_namesapce: str = "gold"
    watermark_namespace: str = "gold.watermarks"
    schema: Union[StructType, None] = None

    def __init__(self,):
        self._dev = True
        self.spark_session = get_spark_session(f"{self.job_name}", dev=self._dev)
        
        # initialize_namespace(self.spark_session, self.watermark_namespace, is_drop=self._dev)
        # self.wartermark_table_identifier = f"{self.watermark_namespace}.{self.dst_table_name}"
        # self.watermark_df = self.spark_session.createDataFrame([], WATERMARK_SCHEMA)
        # write_iceberg(self.spark_session, self.watermark_df, self.wartermark_table_identifier, mode='a')
        # self.watermark_df = self.spark_session.read.table(self.wartermark_table_identifier)
        
        self.dst_table_identifier: str = f"{self.dst_namesapce}.{self.dst_table_name}"
        self.dst_df = self.spark_session.createDataFrame([], schema=self.schema)
        write_iceberg(self.spark_session, self.dst_df, self.dst_table_identifier, mode='a')

class SalesAggregator(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'sales'
        self.schema = SALES
        super().__init__()

    def generate(self,):
        delivered_order_df = self.spark_session.read.table(f"{self.src_namespace}.delivered_order")

        product_metadata_df = self.spark_session.read.table(f"{self.src_namespace}.product_metadata")
        product_category_df = product_metadata_df.select('product_id', 'category')

        order_transaction_df = self.spark_session.read.table(f"{self.src_namespace}.order_transaction")

        fact_sales_df = delivered_order_df \
            .join(order_transaction_df, on='order_id', how='inner') \
            .join(product_category_df, on='product_id', how='inner')
        
        self.output_df = fact_sales_df.groupBy('product_id', 'category').agg(
            F.count(F.col('order_id')).alias('sold_count'),
            F.round(F.sum(F.col('price')), 5).alias('total_sales')
            )
        
        self.output_df = self.output_df.withColumn('mean_sales', F.col('total_sales') / F.col('sold_count'))
        
    def update_table(self,):
        self.output_df.createOrReplaceTempView(self.dst_table_name)
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_table_identifier} t
            USING {self.dst_table_name} s
            ON t.product_id = s.product_id
            WHEN MATCHED AND t.sold_count != s.sold_count THEN
                UPDATE SET sold_count = s.sold_count

            WHEN MATCHED AND t.total_sales != s.total_sales THEN
                UPDATE SET total_sales = s.total_sales
            
            WHEN MATCHED AND t.mean_sales != s.mean_sales THEN
                UPDATE SET mean_sales = s.mean_sales
            
            WHEN NOT MATCHED THEN
                INSERT (product_id, category, sold_count, total_sales, mean_sales)
                VALUES (s.product_id, s.category, s.sold_count, s.total_sales, s.mean_sales)
        """)

class ProductPortfolioMatrix(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'sales_bcg'
        self.schema = PRODUCT_PORTFOLIO_MATRIX
        super().__init__()

    def generate(self,):
        """
        목적: 매출 데이터를 기반으로 카테고리 별 매출이 있는 제품을 4개 그룹으로 분류하는 제품 포트폴리오 매트릭스 생성
        
        분류 기준:
            - 판매량 기준점: 카테고리 내 75 백분위수 (상위 25%)
            - 가격 기준점: 카테고리 내 중앙값 (50 백분위수)
        
        분류 그룹:
            - Star Products: 높은 판매량 + 높은 가격 (고수익 인기 제품)
            - Volume Drivers: 높은 판매량 + 낮은 가격 (대량 판매 제품)
            - Niche Gems: 낮은 판매량 + 높은 가격 (프리미엄 틈새 제품)
            - Question Marks: 낮은 판매량 + 낮은 가격 (저성과 제품)
        """

        sales_df = self.spark_session.read.table("gold.sales")
        
        distinct_category = sales_df.select('category').distinct().collect()
        distinct_category_list = [row.category for row in distinct_category]

        self.output_df = self.spark_session.createDataFrame([], schema=self.schema)

        for category in distinct_category_list:
            src_df = sales_df.filter(F.col('category') == category)

            # 1. 기준점(Threshold) 계산
            # percentile_approx 함수를 사용하여 분위수 계산
            order_count_threshold = src_df.agg(
                F.expr("percentile_approx(sold_count, 0.75)")
            ).collect()[0][0]

            median_avg_price = src_df.agg(
                F.expr("percentile_approx(mean_sales, 0.5)")
            ).collect()[0][0]

            # 2. 'group' 컬럼 추가
            bcg_output = src_df.withColumn("group",
                F.when((F.col("sold_count") >= order_count_threshold) & (F.col("mean_sales") >= median_avg_price), "Star Products")
                .when((F.col("sold_count") >= order_count_threshold) & (F.col("mean_sales") < median_avg_price), "Volume Drivers")
                .when((F.col("sold_count") < order_count_threshold) & (F.col("mean_sales") >= median_avg_price), "Niche Gems")
                .otherwise("Question Marks")
            )

            self.output_df = self.output_df.union(bcg_output)

    def update_table(self,):
        # TODO: compare time-cost with mode 'w' and `upsert`
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='w')

class DeliveredOrderLocation(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'delivered_order_location'
        self.schema = DELIVERED_ORDER_LOCATION
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)
        delivered_order_df = self.spark_session.read.table(f"{self.src_namespace}.delivered_order").select('order_id')
        delivered_order_df = delivered_order_df.join(self.dst_df, on='order_id', how='left_anti')

        geolocation_df = self.spark_session.read.table(f"{self.src_namespace}.geolocation")
        coord = geolocation_df.select('zip_code', 'lng', 'lat')
        
        # seller location
        product_metadata_df = self.spark_session.read.table(f"{self.src_namespace}.product_metadata")
        seller_df = self.spark_session.read.table(f"{self.src_namespace}.seller")

        order_transaction_df = self.spark_session.read.table(f"{self.src_namespace}.order_transaction")
        order_product = order_transaction_df.select('order_id', 'product_id').dropDuplicates()
        
        seller_location_df = delivered_order_df \
            .join(order_product, on='order_id', how='left') \
            .join(product_metadata_df, on='product_id', how='inner') \
            .join(seller_df, on='seller_id', how='inner') \
            .join(coord, on='zip_code', how='inner') \
            .withColumnRenamed('seller_id', 'user_id') \
            .withColumn('user_type', F.lit('seller'))

        # customer location
        order_customer_df = self.spark_session.read.table(f"{self.src_namespace}.order_customer")
        customer_df = self.spark_session.read.table(f"{self.src_namespace}.customer")
        
        customer_location_df = delivered_order_df \
            .join(order_customer_df, on='order_id', how='inner') \
            .join(customer_df, on='customer_id', how='inner') \
            .join(coord, on='zip_code', how='inner') \
            .withColumnRenamed('customer_id', 'user_id') \
            .withColumn('user_type', F.lit('customer'))
        
        # concat product_id and category
        # 동일한 제품에 여러 판매자가 있을 수 있으므로, seller_location_df.dropDuplicates() 수행
        # 동일 제품의 여러 구매자가 있을 수 있음
        customer_location_df = seller_location_df.select('order_id', 'product_id', 'category').dropDuplicates() \
            .join(customer_location_df, on='order_id', how='inner')
        
        self.output_df = customer_location_df.union(seller_location_df).drop('zip_code')

    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')

class OrderLeadDays(GoldBatchJob):
    def __init__(self):
        self.job_name = self.__class__.__name__
        self.dst_table_name = 'order_lead_days'
        self.schema = ORDER_LEAD_DAYS
        super().__init__()

    def generate(self,):
        self.dst_df = self.spark_session.read.table(self.dst_table_identifier)
        delivered_order_df = self.spark_session.read.table(f"{self.src_namespace}.delivered_order")
        delivered_order_df = delivered_order_df.join(self.dst_df, on='order_id', how='left_anti')

        order_timeline_df = self.spark_session.read.table(f"{self.src_namespace}.order_timeline")
        complete_order_timeline = delivered_order_df.join(order_timeline_df, on='order_id', how='left')
        
        self.output_df = complete_order_timeline \
            .withColumn(
                'approve',
                F.datediff(F.col('approve_timestamp'), F.col('purchase_timestamp'))) \
            .withColumn(
                'delivered_carrier',
                F.datediff(F.col('delivered_carrier_timestamp'), F.col('approve_timestamp'))) \
            .withColumn(
                'delivered_customer',
                F.datediff(F.col('delivered_customer_timestamp'), F.col('delivered_carrier_timestamp'))) \
            .withColumn(
                'total_delivery',
                F.datediff(F.col('delivered_customer_timestamp'), F.col('purchase_timestamp'))) \
            .withColumn(
                'is_late_delivery',
                F.when(F.col('delivered_customer_timestamp') <= F.col('estimated_delivery_timestamp'), 'on_time')
                .otherwise('late')) \
            .withColumn(
                'is_late_shipping',
                F.when(F.col('shipping_limit_timestamp') < F.col('delivered_carrier_timestamp'), 'late')
                .otherwise('on_time'))
        
        self.output_df = self.output_df.select(
            'order_id',
            'approve',
            'delivered_carrier',
            'delivered_customer',
            'total_delivery',
            'is_late_delivery',
            'is_late_shipping'
        )
        
    def update_table(self,):
        write_iceberg(self.spark_session, self.output_df, self.dst_table_identifier, mode='a')