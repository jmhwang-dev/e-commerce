from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from .base import BaseBatch
from service.utils.schema.reader import AvscReader
from service.utils.schema.avsc import SilverAvroSchema, GoldAvroSchema

class DimUserLocationBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.DIM_USER_LOCATION, spark_session)

    def extract(self):
        geo_coord_avsc_reader = AvscReader(SilverAvroSchema.GEO_COORD)
        self.check_table(self.spark_session, geo_coord_avsc_reader.dst_table_identifier)
        self.geo_coord_df = self.spark_session.read.table(geo_coord_avsc_reader.dst_table_identifier)

        olist_user_avsc_reader = AvscReader(SilverAvroSchema.OLIST_USER)
        self.check_table(self.spark_session, olist_user_avsc_reader.dst_table_identifier)
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

class FactOrderLeadDaysBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_ORDER_LEAD_DAYS, spark_session)

    def extract(self):
        order_event_avsc_reader = AvscReader(SilverAvroSchema.ORDER_EVENT)
        self.check_table(self.spark_session, order_event_avsc_reader.dst_table_identifier)
        self.order_event_df = self.spark_session.read.table(order_event_avsc_reader.dst_table_identifier)

    def transform(self,):
        # self.output_df = FactOrderLeadDaysBase.transform(self.order_event_df)
        order_timeline_df = self.order_event_df \
            .groupBy('order_id') \
            .agg(
                F.max(F.when(F.col('data_type') == 'purchase', F.col('timestamp'))).alias('purchase'),
                F.max(F.when(F.col('data_type') == 'approve', F.col('timestamp'))).alias('approve'),
                F.max(F.when(F.col('data_type') == 'delivered_carrier', F.col('timestamp'))).alias('delivered_carrier'),
                F.max(F.when(F.col('data_type') == 'delivered_customer', F.col('timestamp'))).alias('delivered_customer'),
                F.max(F.when(F.col('data_type') == 'shipping_limit', F.col('timestamp'))).alias('shipping_limit'),
                F.max(F.when(F.col('data_type') == 'estimated_delivery', F.col('timestamp'))).alias('estimated_delivery'),
            )

        self.output_df = order_timeline_df.withColumns({
            "purchase_to_approval_days": F.date_diff("approve", "purchase"),
            "approval_to_carrier_days": F.date_diff("delivered_carrier", "approve"),
            "carrier_to_customer_days": F.date_diff("delivered_customer", "delivered_carrier"),
            "carrier_delivery_delay_days": F.date_diff("delivered_carrier", "shipping_limit"),
            "customer_delivery_delay_days": F.date_diff("delivered_customer", "estimated_delivery")
        })

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
                    t.estimated_delivery = COALESCE(s.estimated_delivery, t.estimated_delivery),
                    t.purchase_to_approval_days = COALESCE(s.purchase_to_approval_days, t.purchase_to_approval_days),
                    t.approval_to_carrier_days = COALESCE(s.approval_to_carrier_days, t.approval_to_carrier_days),
                    t.carrier_to_customer_days = COALESCE(s.carrier_to_customer_days, t.carrier_to_customer_days),
                    t.carrier_delivery_delay_days = COALESCE(s.carrier_delivery_delay_days, t.carrier_delivery_delay_days),
                    t.customer_delivery_delay_days = COALESCE(s.customer_delivery_delay_days, t.customer_delivery_delay_days)
            WHEN NOT MATCHED THEN
                INSERT *
            """)
        self.get_current_dst_table(output_df.sparkSession, batch_id)

class OrderDetailBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.ORDER_DETAIL, spark_session)

    def extract(self):
        customer_order_avsc_reader = AvscReader(SilverAvroSchema.CUSTOMER_ORDER)
        self.check_table(self.spark_session, customer_order_avsc_reader.dst_table_identifier)
        self.customer_order_df = self.spark_session.read.table(customer_order_avsc_reader.dst_table_identifier)

        product_metadata_avsc_reader = AvscReader(SilverAvroSchema.PRODUCT_METADATA)
        self.check_table(self.spark_session, product_metadata_avsc_reader.dst_table_identifier)
        self.product_metadata_df = self.spark_session.read.table(product_metadata_avsc_reader.dst_table_identifier)

    def transform(self,):

        joined_df = self.customer_order_df.alias('co').join(
            self.product_metadata_df.alias('pm'),
            on=['product_id', 'seller_id'],
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
            
            WHEN MATCHED and t.quantity != s.quantity or t.category != s.category THEN
                UPDATE SET
                    t.quantity = s.quantity,
                    t.category = s.category

            WHEN NOT MATCHED THEN
                INSERT (order_id, product_id, category, quantity, unit_price, user_id)
                VALUES (s.order_id, s.product_id, s.category, s.quantity, s.unit_price, s.user_id)
            """)
        
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)

class FactMonthlySalesByProductBatch(BaseBatch):
    """
    목적: 월별 제품 매출 기록을 기반으로 카테고리별로 4개 그룹으로 분류된 제품 포트폴리오 매트릭스 생성
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
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT, spark_session)

    def extract(self):
        fact_order_timeline_avsc_reader = AvscReader(GoldAvroSchema.FACT_ORDER_LEAD_DAYS)
        self.check_table(self.spark_session, fact_order_timeline_avsc_reader.dst_table_identifier)
        self.fact_order_lead_days = self.spark_session.read.table(fact_order_timeline_avsc_reader.dst_table_identifier)

        order_detail_avsc_reader = AvscReader(GoldAvroSchema.ORDER_DETAIL)
        self.check_table(self.spark_session, order_detail_avsc_reader.dst_table_identifier)
        self.order_detail_df = self.spark_session.read.table(order_detail_avsc_reader.dst_table_identifier)

    def transform(self):
        sales_period_df = self.fact_order_lead_days \
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
        
        fact_monthly_sales_by_product_df = product_period_sales_df.withColumn(
            'mean_sales', 
            F.col('total_sales_amount') / F.col('total_sales_quantity')
        )

        # 1. 매출 기준 사분위수 도출
        sales_quantile_df = fact_monthly_sales_by_product_df.groupBy('category', 'sales_period').agg(
            F.percentile_approx('total_sales_amount', 0.75).alias('q3'),
            F.percentile_approx('total_sales_amount', 0.5).alias('q2'),
            F.percentile_approx('total_sales_amount', 0.25).alias('q1')
        )

        # 2. 사분위수 기준 매출 그룹 분류
        self.output_df = fact_monthly_sales_by_product_df.join(
            sales_quantile_df, 
            on=['category', 'sales_period'], 
            how='inner'
        ).withColumn(
            "group",
            F.when(F.col("q3") <= (F.col("total_sales_amount")), "Star Products")
             .when((F.col("q2") <= F.col("total_sales_amount")) & (F.col("total_sales_amount") < F.col("q3")), "Volume Drivers")
             .when((F.col("q1") <= F.col("total_sales_amount")) & (F.col("total_sales_amount") < F.col("q2")), "Niche Gems")
             .otherwise("Question Marks")
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
                target.category = source.category,
                target.group = source.group
            WHEN NOT MATCHED THEN
            INSERT (product_id, sales_period, total_sales_quantity, total_sales_amount, mean_sales, category, group)
            VALUES (source.product_id, source.sales_period, source.total_sales_quantity, source.total_sales_amount, source.mean_sales, source.category, source.group);
        """)
        self.get_current_dst_table(output_df.sparkSession, batch_id, False)
        
class FactReviewAnswerLeadDaysBatch(BaseBatch):
    def __init__(self, spark_session: Optional[SparkSession] = None):
        super().__init__(self.__class__.__name__, GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS, spark_session)

    def extract(self):
        review_metadata_avsc_reader = AvscReader(SilverAvroSchema.REVIEW_METADATA)
        self.check_table(self.spark_session, review_metadata_avsc_reader.dst_table_identifier)
        self.review_metadata_df = self.spark_session.read.table(review_metadata_avsc_reader.dst_table_identifier)

    def transform(self):
        self.output_df = self.review_metadata_df.withColumn('until_answer_lead_days',F.date_diff('review_answer_timestamp', 'review_creation_date'))
        
    def load(self):
        self.output_df.createOrReplaceTempView("updates")
        self.spark_session.sql(f"""
            MERGE INTO {self.dst_avsc_reader.dst_table_identifier} target
            USING updates source
            ON target.review_id = source.review_id AND target.order_id = source.order_id
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        self.get_current_dst_table(self.output_df.sparkSession, debug=self.is_debug)