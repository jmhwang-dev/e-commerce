from service.init.kafka import *
from service.producer.utils import *
from service.producer.raw import *

import schema

if __name__=="__main__":
    
    admin_client = get_kafka_admin_client(BOOTSTRAP_SERVERS_EXTERNAL)
    for topic_class in [RawToBronzeTopic, BronzeToSilverTopic]:
        topic_names = topic_class.get_all_topics()
        delete_topics(admin_client, topic_names)
        create_topics(admin_client, topic_names)
    schema.register()

    while not OrderStatusMessage.is_end():
        order_status_log = OrderStatusMessage.get_current_event()
        OrderStatusMessage.publish(order_status_log)

        status = order_status_log['status']
        order_id = order_status_log['order_id']
        
        if status == 'purchase':
            payment_log = PaymentMessage.select('order_id', order_id)
            PaymentMessage.publish(payment_log)

            customer_id = payment_log['customer_id'].iloc[0]
            customer_log = CustomerMessage.select('customer_id', customer_id)
            CustomerMessage.publish(customer_log)

            zip_code = customer_log['zip_code'].iloc[0]
            geolcation = GeolocationMessage.select('zip_code', zip_code)
            GeolocationMessage.publish(geolcation)

            # order_item - cdc
            order_item_log = OrderItemMessage.select('order_id', order_id)
            if order_item_log.empty:
                print(f'\nEmpty message: {OrderItemMessage.topic}')
                continue
        
            OrderItemMessage.publish(order_item_log)

            # product - cdc
            product_id = order_item_log['product_id'].iloc[0]
            proudct_log = ProductMessage.select('product_id', product_id)
            ProductMessage.publish(proudct_log)

            # seller - cdc
            seller_id = order_item_log['seller_id'].iloc[0]
            seller_log = SellerMessage.select('seller_id', seller_id)
            SellerMessage.publish(seller_log)

            # geolocation - cdc
            zip_code = seller_log['zip_code'].iloc[0]
            geolcation = GeolocationMessage.select('zip_code', zip_code)
            GeolocationMessage.publish(geolcation)

        elif status == 'approved':
            estimated_date = EstimatedDeliberyDateMessage.select('order_id', order_id)
            EstimatedDeliberyDateMessage.publish(estimated_date)

        review_log = ReviewMessage.select('order_id', order_id)
        ReviewMessage.publish(review_log)