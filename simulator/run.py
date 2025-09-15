from service.stream.topic import *
from service.utils.schema.registry_manager import *
from service.producer.bronze import *
from service.utils.kafka import *

import time

if __name__=="__main__":
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    topic_names = BronzeTopic.get_all_topics()
    delete_topics(admin_client, topic_names)
    create_topics(admin_client, topic_names)
    register_schema()

    base_interval = 10  # seconds
    order_status_df = OrderStatusBronzeProducer.get_df()
    end_timestamp = order_status_df.loc[0, 'timestamp'] - pd.Timedelta(seconds=1)
    for i, order_status_series in order_status_df.iterrows():
        current_timestamp = order_status_series['timestamp']
        diff_time = current_timestamp - end_timestamp

        # transaction replay: mock real-time transaction
        if diff_time > pd.Timedelta(seconds=base_interval):
            time.sleep(base_interval)
        else:
            time.sleep(diff_time.total_seconds())

        OrderStatusBronzeProducer.publish(order_status_series)
        status = order_status_series['status']

        if status == 'purchase':
            payment_log = PaymentBronzeProducer.select(order_status_series, 'order_id')
            PaymentBronzeProducer.publish(payment_log)

            order_item_log = OrderItemBronzeProducer.select(order_status_series, 'order_id')
            OrderItemBronzeProducer.publish(order_item_log)

            customer_log = CustomerBronzeProducer.select(payment_log, 'customer_id')
            CustomerBronzeProducer.publish(customer_log)

            geolcation = GeolocationBronzeProducer.select(customer_log, 'zip_code')
            GeolocationBronzeProducer.publish(geolcation)

            proudct_log = ProductBronzeProducer.select(order_item_log, 'product_id')
            ProductBronzeProducer.publish(proudct_log)

            seller_log = SellerBronzeProducer.select(order_item_log, 'seller_id')
            SellerBronzeProducer.publish(seller_log)

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateBronzeProducer.select(order_status_series, 'order_id')
            EstimatedDeliberyDateBronzeProducer.publish(estimated_date)

        review_log = ReviewBronzeProducer.select(order_status_series, current_timestamp)
        ReviewBronzeProducer.publish(review_log)

        end_timestamp = current_timestamp