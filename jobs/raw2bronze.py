from service.common.topic import *
from service.producer.bronze import *
from service.common.schema import *

import time

if __name__=="__main__":
    
    admin_client = get_confluent_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    for topic_class in [RawToBronzeTopic, BronzeToSilverTopic]:
        topic_names = topic_class.get_all_topics()
        delete_topics(admin_client, topic_names)
        create_topics(admin_client, topic_names)
    
    register_schema()
    # exit()

    while not OrderStatusBronzeProducer.is_end():
        time.sleep(5)
        order_status_log = OrderStatusBronzeProducer.get_current_event()
        OrderStatusBronzeProducer.publish(order_status_log)

        status = order_status_log['status']
        order_id = order_status_log['order_id']
        
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

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateBronzeProducer.select('order_id', order_id)
            EstimatedDeliberyDateBronzeProducer.publish(estimated_date)

        review_log = ReviewBronzeProducer.select('order_id', order_id)
        ReviewBronzeProducer.publish(review_log)