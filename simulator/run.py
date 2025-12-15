from service.utils.schema.avsc import *
from service.utils.schema.registry_manager import *
from service.producer.bronze import *
from service.utils.kafka import *

import time

if __name__=="__main__":
    all_avsc_filenames = BronzeAvroSchema.get_all_filenames() + SilverAvroSchema.get_all_filenames() + GoldAvroSchema.get_all_filenames()
    SchemaRegistryManager.delete_subject(all_avsc_filenames, False)
    register_schema()
    
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    stream_avsc_filenames = BronzeAvroSchema.get_all_filenames() + SilverAvroSchema.get_all_filenames()
    
    delete_topics(admin_client, stream_avsc_filenames)
    create_topics(admin_client, stream_avsc_filenames, 1, 1)

    base_interval = 30  # seconds
    threshold_interval = 300  # seconds

    order_status_df = OrderStatusBronzeProducer.get_df()
    past_event_timestamp = pd.to_datetime("2016-09-04 21:15:19.000000")   # first timestamp in order_status
    for i, order_status_series in order_status_df.iterrows():
        current_event_timestamp = order_status_series['timestamp']
        event_term = current_event_timestamp - past_event_timestamp

        # transaction replay: mock real-time transaction
        if event_term > pd.Timedelta(seconds=threshold_interval):
            time.sleep(base_interval)
        else:
            time.sleep(event_term.total_seconds())
        
        mock_order_status_series, current_ingest_time = PandasProducer.add_mock_ingest_time(order_status_series, current_event_timestamp)
        OrderStatusBronzeProducer.publish(mock_order_status_series)
        status = mock_order_status_series['status']
        
        if status == 'purchase':
            payment_log = PaymentBronzeProducer.select(order_status_series, ['order_id'])
            mock_payment_log = PandasProducer.calc_mock_ingest_time(payment_log)
            PaymentBronzeProducer.publish(mock_payment_log)

            order_item_log = OrderItemBronzeProducer.select(order_status_series, ['order_id'])
            mock_order_item_log, current_ingest_time = PandasProducer.add_mock_ingest_time(order_item_log, current_ingest_time)
            OrderItemBronzeProducer.publish(mock_order_item_log)

            proudct_log = ProductBronzeProducer.select(order_item_log, ['product_id'])
            mock_proudct_log, current_ingest_time = PandasProducer.add_mock_ingest_time(proudct_log, current_ingest_time)
            ProductBronzeProducer.publish(mock_proudct_log)

            seller_log = SellerBronzeProducer.select(order_item_log, ['seller_id'])
            mock_seller_log, current_ingest_time = PandasProducer.add_mock_ingest_time(seller_log, current_ingest_time)
            SellerBronzeProducer.publish(mock_seller_log)

            geolcation_seller = GeolocationBronzeProducer.select(seller_log, ['zip_code'])
            mock_geolcation_seller, current_ingest_time = PandasProducer.add_mock_ingest_time(geolcation_seller, current_ingest_time)
            GeolocationBronzeProducer.publish(mock_geolcation_seller)

            customer_log = CustomerBronzeProducer.select(payment_log, ['customer_id'])
            mock_customer_log, current_ingest_time = PandasProducer.add_mock_ingest_time(customer_log, current_ingest_time)
            CustomerBronzeProducer.publish(mock_customer_log)

            geolcation_customer = GeolocationBronzeProducer.select(customer_log, ['zip_code'])
            mock_geolcation_customer, current_ingest_time = PandasProducer.add_mock_ingest_time(geolcation_customer, current_ingest_time)
            GeolocationBronzeProducer.publish(mock_geolcation_customer)

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateBronzeProducer.select(order_status_series, ['order_id'])
            mock_estimated_date, _ = PandasProducer.add_mock_ingest_time(estimated_date, current_ingest_time)
            EstimatedDeliberyDateBronzeProducer.publish(mock_estimated_date)

        review_log = ReviewBronzeProducer.select(current_event_timestamp)
        mock_review_log = PandasProducer.calc_mock_ingest_time(review_log)
        ReviewBronzeProducer.publish(mock_review_log)

        past_event_timestamp = current_event_timestamp