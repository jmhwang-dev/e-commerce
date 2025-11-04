from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from abc import ABC, abstractmethod

class CommonSilverTask(ABC):
    @abstractmethod
    def transform(self, ):
        pass

class GeoCoordBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, geo_df: DataFrame):

        # 브라질 위경도 범위 상수
        BRAZIL_BOUNDS = {
            "min_lat": -33.69111,
            "max_lat": 2.81972,
            "min_lon": -72.89583,
            "max_lon": -34.80861
        }

        return geo_df \
            .dropDuplicates(['zip_code']) \
            .filter(
                (F.col("lat").between(BRAZIL_BOUNDS["min_lat"], BRAZIL_BOUNDS["max_lat"])) &
                (F.col("lng").between(BRAZIL_BOUNDS["min_lon"], BRAZIL_BOUNDS["max_lon"]))
            )

class OlistUserBase(CommonSilverTask):

    @classmethod
    def transform(cls, customer_df:DataFrame, seller_df:DataFrame ):
        _customer_df = customer_df \
            .withColumnRenamed('customer_id', 'user_id') \
            .withColumn('user_type', F.lit('customer')) \
            .drop('ingest_time') \
            .dropDuplicates()

        _seller_df = seller_df \
            .withColumnRenamed('seller_id', 'user_id') \
            .withColumn('user_type', F.lit('seller')) \
            .drop('ingest_time') \
            .dropDuplicates()
        
        return _customer_df.unionByName(_seller_df)
    
class OrderEventBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, estimated_df:DataFrame, shippimt_limit_df:DataFrame, order_status_df:DataFrame):
        # TODO: chage timezone to UTC. (현재는 KST로 들어감)
        _estimated_df = estimated_df \
            .withColumnRenamed('estimated_delivery_date', 'timestamp') \
            .withColumn('data_type', F.lit('estimated_delivery'))
        
        _shippimt_limit_df = shippimt_limit_df \
            .select('order_id', 'shipping_limit_date', 'ingest_time') \
            .withColumnRenamed('shipping_limit_date', 'timestamp') \
            .withColumn('data_type', F.lit('shipping_limit'))
        
        _order_status_df = order_status_df.withColumnRenamed('status', 'data_type')

        return _order_status_df.unionByName(_estimated_df).unionByName(_shippimt_limit_df)
    
class ProductMetadataBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, product_df:DataFrame, order_item_df:DataFrame):
        product_category = product_df \
            .select('product_id', 'category', 'ingest_time') \
            .dropna() \
            .dropDuplicates()
            
        product_seller = order_item_df \
            .select('product_id', 'seller_id', 'ingest_time') \
            .dropDuplicates()
        
        return product_category.alias('p_cat').join(
            product_seller.alias('p_sel'),
            on=F.expr("""
                p_cat.product_id = p_sel.product_id and
                p_cat.ingest_time >= p_sel.ingest_time - interval 30 days AND
                p_cat.ingest_time <= p_sel.ingest_time + interval 30 days
            """),
            how='fullouter'
            ).select(
                F.col('p_cat.category').alias('category'),
                F.col('p_cat.product_id').alias('product_id'),
                F.col('p_sel.seller_id').alias('seller_id'),
            ).dropDuplicates() \
            .dropna()
    

class OrderDetailBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, order_item_df:DataFrame, payment_df:DataFrame):
        """
        Non-windowed groupBy: 전체 스트림 기간 동안 누적 집로, 상태가 무한이 증가한다. (unbounded state)
        - watermark는 배치에서 late data를 처리하는 목적이다.
        - 따라서 watermark가 있어도 상태를 drop할 시간 경계가 없어서 상태가 finalized 되지 않음
        - 결국 OOM으로 이어질 가능성이 매우 높아지기 때문에, 다음과 같은 예외를 발생시킨다.
        (AnalysisException: "Append output mode not supported for streaming aggregations without watermark")

        대안1. window 함수

        대안2. Append 대신 update (변경된 행만 출력) 또는 complete (전체 결과 재출력) mode로 전환
        - 상태 drop: Update mode에서 watermark 설정 시 오래된 상태 정리 가능 (complete는 모든 상태 유지, drop 안 함).
        - 한계: Append가 필요한 싱크 (e.g., Kafka append-only)에서 불가. Complete는 대규모 데이터에서 비효율적 (전체 재출력).

        대안 3: mapGroupsWithState 또는 flatMapGroupsWithState로 커스텀 상태 관리
        """

        # order_customer state
        order_item_price = order_item_df.select('order_id', 'order_item_id', 'product_id', 'price', 'ingest_time').dropna()
        aggregated_df = order_item_price.groupBy(
            F.window("ingest_time", "5 minutes"),  # 필수: non-windowed → windowed
            "order_id", "product_id", "price") \
            .agg(
                F.count("order_item_id").alias("quantity"),
                F.max("ingest_time").alias('agg_ingest_time')
            ).withColumnRenamed("price", "unit_price") \
            .drop('window')

        order_customer = payment_df.groupBy(
            F.window("ingest_time", "5 minutes"),  # 필수: non-windowed → windowed
            'order_id', 'customer_id').agg(F.max('ingest_time').alias('oc_ingest_time')) \
            .drop('window')
        
        return aggregated_df.alias('agg').join(
                order_customer.alias('oc'),
                F.expr("""
                    agg.order_id = oc.order_id AND
                """),
                "inner"
            ).select(
                F.col('agg.order_id').alias('order_id'), 'customer_id', 'product_id', 'unit_price', 'quantity'
            )
    
class ReviewMetadataBase(CommonSilverTask):
    
    @classmethod
    def transform(cls, review_stream:DataFrame):
        return review_stream \
            .drop('review_comment_title', 'review_comment_message', 'ingest_time') \
            .dropDuplicates()