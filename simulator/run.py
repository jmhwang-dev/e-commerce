from service.common.topic import *
from service.utils.schema.registry_manager import *
from service.producer.bronze import *
from service.utils.kafka import *

import time

if __name__=="__main__":
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    topic_names = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics() + DeadLetterQueuerTopic.get_all_topics() + InferenceTopic.get_all_topics()
    delete_topics(admin_client, topic_names)
    create_topics(admin_client, topic_names)
    register_schema()

    base_interval = 5  # seconds
    order_status_df = OrderStatusBronzeProducer.get_df()
    current_timestamp = order_status_df.iloc[0, 0] - pd.Timedelta(seconds=1)
    for i, order_status_series in order_status_df.iterrows():
        new_timestamp = order_status_series['timestamp']
        diff_time = new_timestamp - current_timestamp

        # data replay: mock real-time
        if diff_time > pd.Timedelta(seconds=base_interval):
            time.sleep(base_interval)
        else:
            time.sleep(diff_time.total_seconds())

        current_timestamp = new_timestamp

        order_status_log, status, order_id = \
            OrderStatusBronzeProducer.mock_order_status_log(order_status_series)
        
        OrderStatusBronzeProducer.publish(order_status_log)

        if status == 'purchase':
            payment_log = PaymentBronzeProducer.select('order_id', order_id)
            PaymentBronzeProducer.publish(payment_log)

            customer_id = payment_log['customer_id'].iloc[0]
            customer_log = CustomerBronzeProducer.select('customer_id', customer_id)
            CustomerBronzeProducer.publish(customer_log)

            zip_code = customer_log['zip_code'].iloc[0]
            geolcation = GeolocationBronzeProducer.select('zip_code', zip_code)
            GeolocationBronzeProducer.publish(geolcation)

            # order_item - cdc
            order_item_log = OrderItemBronzeProducer.select('order_id', order_id)
            if order_item_log.empty:
                print(f'\nEmpty message: {OrderItemBronzeProducer.topic}')
                continue
        
            OrderItemBronzeProducer.publish(order_item_log)

            # product - cdc
            product_id = order_item_log['product_id'].iloc[0]
            proudct_log = ProductBronzeProducer.select('product_id', product_id)
            ProductBronzeProducer.publish(proudct_log)

            # seller - cdc
            seller_id = order_item_log['seller_id'].iloc[0]
            seller_log = SellerBronzeProducer.select('seller_id', seller_id)
            SellerBronzeProducer.publish(seller_log)

            # geolocation - cdc
            zip_code = seller_log['zip_code'].iloc[0]
            geolcation = GeolocationBronzeProducer.select('zip_code', zip_code)
            GeolocationBronzeProducer.publish(geolcation)

            review_log = ReviewBronzeProducer.select('order_id', order_id)
            ReviewBronzeProducer.publish(review_log)

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateBronzeProducer.select('order_id', order_id)
            EstimatedDeliberyDateBronzeProducer.publish(estimated_date)

        