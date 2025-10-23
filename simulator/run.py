from service.stream.topic import *
from service.utils.schema.registry_manager import *
from service.producer.bronze import *
from service.utils.kafka import *

import time

if __name__=="__main__":
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    topic_names = BronzeTopic.get_all_topics()
    delete_topics(admin_client, topic_names)
    SchemaRegistryManager.delete_subject(topic_names, False)
    create_topics(admin_client, topic_names)
    register_schema()

    base_interval = 10  # seconds
    order_status_df = OrderStatusBronzeProducer.get_df()
    past_event_timestamp = pd.to_datetime("2016-09-04 21:15:19.000000")   # first timestamp in order_status
    for i, order_status_series in order_status_df.iterrows():
        current_event_timestamp = order_status_series['timestamp']
        event_term = current_event_timestamp - past_event_timestamp

        # transaction replay: mock real-time transaction
        if event_term > pd.Timedelta(seconds=base_interval):
            time.sleep(base_interval)
        else:
            time.sleep(event_term.total_seconds())
        
        mock_order_status_series, current_ingest_time = PandasProducer.add_mock_ingest_time(order_status_series, current_event_timestamp)
        OrderStatusBronzeProducer.publish(mock_order_status_series)
        status = mock_order_status_series['status']
        
        if status == 'purchase':
            payment_log = PaymentBronzeProducer.select(order_status_series, 'order_id')
            mock_payment_log = PandasProducer.calc_mock_ingest_time(payment_log)
            PaymentBronzeProducer.publish(mock_payment_log)

            order_item_log = OrderItemBronzeProducer.select(order_status_series, 'order_id')
            mock_order_item_log, current_ingest_time = PandasProducer.add_mock_ingest_time(order_item_log, current_ingest_time)
            OrderItemBronzeProducer.publish(mock_order_item_log)

            customer_log = CustomerBronzeProducer.select(payment_log, 'customer_id')
            mock_customer_log, current_ingest_time = PandasProducer.add_mock_ingest_time(customer_log, current_ingest_time)
            CustomerBronzeProducer.publish(mock_customer_log)

            geolcation = GeolocationBronzeProducer.select(customer_log, 'zip_code')
            mock_geolcation, current_ingest_time = PandasProducer.add_mock_ingest_time(geolcation, current_ingest_time)
            GeolocationBronzeProducer.publish(mock_geolcation)

            proudct_log = ProductBronzeProducer.select(order_item_log, 'product_id')
            mock_proudct_log, current_ingest_time = PandasProducer.add_mock_ingest_time(proudct_log, current_ingest_time)
            ProductBronzeProducer.publish(mock_proudct_log)

            seller_log = SellerBronzeProducer.select(order_item_log, 'seller_id')
            mock_seller_log, current_ingest_time = PandasProducer.add_mock_ingest_time(seller_log, current_ingest_time)
            SellerBronzeProducer.publish(mock_seller_log)

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateBronzeProducer.select(order_status_series, 'order_id')
            mock_estimated_date, _ = PandasProducer.add_mock_ingest_time(estimated_date, current_ingest_time)
            EstimatedDeliberyDateBronzeProducer.publish(mock_estimated_date)

        review_log = ReviewBronzeProducer.select(order_status_series, current_event_timestamp)
        mock_review_log = PandasProducer.calc_mock_ingest_time(review_log)
        ReviewBronzeProducer.publish(mock_review_log)

        past_event_timestamp = current_event_timestamp