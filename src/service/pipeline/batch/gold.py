from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseBatch
from service.pipeline.common.gold import *
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema

class DimUserLocationBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.DIM_USER_LOCATION, spark_session)

    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.geo_coord_df = self.spark_session.read.table(geo_coord_avsc_reader.dst_table_identifier)

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.olist_user = self.spark_session.read.table(olist_user_avsc_reader.dst_table_identifier)

    def transform(self,):
        unique_olist_user_df = self.olist_user.dropDuplicates()
        unique_geo_coord_df = self.geo_coord_df.dropDuplicates()
        self.output_df = unique_geo_coord_df \
            .withColumn('zip_code', F.col('zip_code').cast(IntegerType())) \
            .join(unique_olist_user_df, on='zip_code', how='inner')
        
    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.user_id = s.user_id AND t.zip_code = s.zip_code
            WHEN NOT MATCHED THEN
                INSERT (user_id, zip_code, lng, lat, user_type)
                VALUES (s.user_id, s.zip_code, s.lng, s.lat, s.user_type)
            """)
        
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactOrderTimelineBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_ORDER_TIMELINE, spark_session)

    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.order_event_df = self.spark_session.read.table(order_event_avsc_reader.dst_table_identifier)

    def transform(self,):
        self.output_df = FactOrderTimelineBase.transform(self.order_event_df)

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.purchase = COALESCE(s.purchase, t.purchase),
                    t.approve = COALESCE(s.approve, t.approve),
                    t.delivered_carrier = COALESCE(s.delivered_carrier, t.delivered_carrier),
                    t.delivered_customer = COALESCE(s.delivered_customer, t.delivered_customer),
                    t.shipping_limit = COALESCE(s.shipping_limit, t.shipping_limit),
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery)
            WHEN NOT MATCHED THEN
                INSERT (order_id, purchase, approve, delivered_carrier, delivered_customer, shipping_limit, estimated_delivery)
                VALUES (s.order_id, s.purchase, s.approve, s.delivered_carrier, s.delivered_customer, s.shipping_limit, s.estimated_delivery)
            """)
        self.get_current_dst_table(output_df.sparkSession, batch_id)

class OrderDetailBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.ORDER_DETAIL, spark_session)

    def extract(self):
        customer_order_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.customer_order_df = self.spark_session.read.table(customer_order_avsc_reader.dst_table_identifier)

        product_metadata_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.product_metadata_df = self.spark_session.read.table(product_metadata_avsc_reader.dst_table_identifier)

    def transform(self,):

        joined_df = self.customer_order_df.alias('co').join(
            self.product_metadata_df.alias('pm'),
            on='product_id',
            how='inner')
        
        common_columns = ["order_id", "product_id", "category", "quantity", "unit_price"]
        order_seller_df = joined_df \
            .select(*(common_columns + ['seller_id'])) \
            .withColumnRenamed('seller_id', 'user_id')

        order_customer_df = joined_df \
            .select(*(common_columns + ['customer_id'])) \
            .withColumnRenamed('customer_id', 'user_id')
        
        self.output_df = order_seller_df.unionByName(order_customer_df)
        
    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        output_df.createOrReplaceTempView("updates")
        output_df.sparkSession.sql(
            f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id and t.product_id = s.product_id and t.user_id = s.user_id
            WHEN NOT MATCHED THEN
                INSERT (order_id, user_id, product_id, category, quantity, unit_price)
                VALUES (s.order_id, s.user_id, s.product_id, s.category, s.quantity, s.unit_price)
            """)
        
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactOrderLeadDaysBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_ORDER_LEAD_DAYS, spark_session)

    def extract(self):
        fact_order_timeline_avsc_reader = AvscReader(GoldAvroSchema.FACT_ORDER_TIMELINE)
        self.fact_order_timeline = self.spark_session.read.table(fact_order_timeline_avsc_reader.dst_table_identifier)

    def transform(self,):
        """
        `shipping_delay > 0` means late shipping
        `delivery_customer_delay > 0` means late delivery to customer
        """
        self.output_df = self.fact_order_timeline.select(
            'order_id',
            F.date_diff("approve", "purchase").alias("until_approve"),
            F.date_diff("delivered_carrier", "approve").alias("until_delivered_carrier"),
            F.date_diff("delivered_customer", "delivered_carrier").alias("until_delivered_customer"),
            F.date_diff("delivered_carrier", "shipping_limit").alias("shipping_delay"),
            F.date_diff("delivered_customer", "estimated_delivery").alias("delivery_customer_delay")
        )

        self.output_df = self.output_df.select(
            "order_id",
            "until_approve",
            "until_delivered_carrier",
            "until_delivered_customer",
            "shipping_delay",
            "delivery_customer_delay"
        )

    def load(self,):
        # write_iceberg(self.spark_session, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} t
            USING updates s
            ON t.order_id = s.order_id
            WHEN MATCHED AND t.until_approve != s.until_approve THEN
                UPDATE SET until_approve = s.until_approve

            WHEN MATCHED AND t.until_delivered_carrier != s.until_delivered_carrier THEN
                UPDATE SET until_delivered_carrier = s.until_delivered_carrier
            
            WHEN MATCHED AND t.until_delivered_customer != s.until_delivered_customer THEN
                UPDATE SET until_delivered_customer = s.until_delivered_customer
            
            WHEN MATCHED AND t.shipping_delay != s.shipping_delay THEN
                UPDATE SET shipping_delay = s.shipping_delay
            
            WHEN MATCHED AND t.delivery_customer_delay != s.delivery_customer_delay THEN
                UPDATE SET delivery_customer_delay = s.delivery_customer_delay
            
            WHEN NOT MATCHED THEN
                INSERT *
        """)

class FactMonthlySalesByProductBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT, spark_session)

    def extract(self):
        fact_order_timeline_avsc_reader = AvscReader(GoldAvroSchema.FACT_ORDER_TIMELINE)
        self.fact_order_timeline = self.spark_session.read.table(fact_order_timeline_avsc_reader.dst_table_identifier)

        order_detail_avsc_reader = AvscReader(GoldAvroSchema.ORDER_DETAIL)
        self.order_detail_df = self.spark_session.read.table(order_detail_avsc_reader.dst_table_identifier)

    def transform(self):
        sales_period_df = self.fact_order_timeline \
            .filter(F.col('delivered_customer').isNotNull()) \
            .select('order_id', 'delivered_customer') \
            .withColumn('sales_period', F.date_format(F.col('delivered_customer'), 'yyyy-MM')) \
            .drop('delivered_customer')
        
        period_order_detail_df = sales_period_df.join(self.order_detail_df, on='order_id', how='inner')

        product_period_sales_df = period_order_detail_df.groupBy('product_id', 'category', 'sales_period') \
            .agg(
                F.sum('quantity').alias('total_sales_quantity'),
                F.sum(F.col('quantity') * F.col('unit_price')).alias('total_sales_amount')
            )
        
        self.output_df = product_period_sales_df.withColumn(
            'mean_sales', 
            F.col('total_sales_amount') / F.col('total_sales_quantity')
        )

    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.product_id = source.product_id AND target.sales_period = source.sales_period
            WHEN MATCHED THEN
            UPDATE SET
                target.total_sales_quantity = source.total_sales_quantity,
                target.total_sales_amount = source.total_sales_amount,
                target.mean_sales = source.mean_sales,
                target.category = source.category
            WHEN NOT MATCHED THEN
            INSERT (product_id, sales_period, total_sales_quantity, total_sales_amount, mean_sales, category)
            VALUES (source.product_id, source.sales_period, source.total_sales_quantity, source.total_sales_amount, source.mean_sales, source.category);
        """)
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)
        
class FactProductPeriodPortfolioBatch(BaseBatch):
    """
    목적: 기간별 및 전체 누적 제품 매출 기록을 기반으로 카테고리 별 매출이 있는 제품을 4개 그룹으로 분류하는 제품 포트폴리오 매트릭스 생성
    분류 기준:
        - 판매량 기준점: 카테고리 내 75 백분위수 (상위 25%)
        - 가격 기준점: 카테고리 내 중앙값 (50 백분위수)
    분류 그룹:
        - Star Products: 높은 판매량 + 높은 가격 (고수익 인기 제품)
        - Volume Drivers: 높은 판매량 + 낮은 가격 (대량 판매 제품)
        - Niche Gems: 낮은 판매량 + 높은 가격 (프리미엄 틈새 제품)
        - Question Marks: 낮은 판매량 + 낮은 가격 (저성과 제품)
    """
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_PRODUCT_PERIOD_PORTFOLIO, spark_session)

    def extract(self):
        product_period_sales_metrics_avsc_reader = AvscReader(GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT)
        self.product_period_sales_metrics_df = self.spark_session.read.table(product_period_sales_metrics_avsc_reader.dst_table_identifier)

    def transform(self):
        # 1. 기간별 임계값 계산 (category, sales_period별)
        period_thresholds_df = self.product_period_sales_metrics_df.groupBy('category', 'sales_period').agg(
            F.percentile_approx('total_sales_quantity', 0.75).alias('sold_count_threshold'),
            F.percentile_approx('mean_sales', 0.5).alias('median_avg_price')
        )

        # 2. 기간별 데이터에 임계값 조인 및 그룹 분류
        period_matrix_df = self.product_period_sales_metrics_df.join(
            period_thresholds_df, 
            on=['category', 'sales_period'], 
            how='inner'
        ).withColumn(
            "group",
            F.when((F.col("total_sales_quantity") >= F.col("sold_count_threshold")) & (F.col("mean_sales") >= F.col("median_avg_price")), "Star Products")
             .when((F.col("total_sales_quantity") >= F.col("sold_count_threshold")) & (F.col("mean_sales") < F.col("median_avg_price")), "Volume Drivers")
             .when((F.col("total_sales_quantity") < F.col("sold_count_threshold")) & (F.col("mean_sales") >= F.col("median_avg_price")), "Niche Gems")
             .otherwise("Question Marks")
        ).select('product_id', 'sales_period', 'category', 'total_sales_quantity', 'mean_sales', 'group')  # 필요 컬럼만 선택

        self.output_df = period_matrix_df
        
    def load(self, df:Optional[DataFrame] = None, batch_id: int = -1):
        if df is not None:
            output_df = df
        else:
            output_df = self.output_df

        # write_iceberg(self.spark_session, self.output_df, self.dst_avsc_reader.dst_table_identifier, mode='w')
        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.product_id = source.product_id AND target.sales_period = source.sales_period
            WHEN MATCHED THEN
            UPDATE SET
                target.category = source.category,
                target.total_sales_quantity = source.total_sales_quantity,
                target.mean_sales = source.mean_sales,
                target.group = source.group
            WHEN NOT MATCHED THEN
            INSERT (product_id, sales_period, category, total_sales_quantity, mean_sales, group)
            VALUES (source.product_id, source.sales_period, source.category, source.total_sales_quantity, source.mean_sales, source.group);
        """)
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)


class FactReviewStatsBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_REVIEW_STATS, spark_session)

    def extract(self):
        review_metadata_avsc_reader = AvscReader(SilverAvroSchema.REVIEW_METADATA)
        self.review_metadata_df = self.spark_session.read.table(review_metadata_avsc_reader.dst_table_identifier)

        order_detail_avsc_reader = AvscReader(GoldAvroSchema.ORDER_DETAIL)
        self.order_detail_df = self.spark_session.read.table(order_detail_avsc_reader.dst_table_identifier)

    def transform(self):
        product_review = self.review_metadata_df \
            .join(self.order_detail_df.select('order_id', 'product_id', 'category'), on='order_id', how='inner') \

        self.output_df = product_review.withColumn('until_answer_lead_days',F.date_diff('review_answer_timestamp', 'review_creation_date'))
        
    def load(self):
        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.product_id = source.product_id AND target.review_id = source.review_id AND target.order_id = source.order_id
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        self.get_current_dst_table(self.output_df.sparkSession, debug=self.is_debug)