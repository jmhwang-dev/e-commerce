from service.producer.bronze import *

def get_producer(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer
    
    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer

def get_avro_key_column(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer.key_column

    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer.key_column
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer.key_column
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer.key_column
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer.key_column
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer.key_column
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer.key_column
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer.key_column
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer.key_column
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer.key_column
    
    # silver
    # TODO: resolve hard coding
    elif topic_name == 'geo_coord':
        return 'zip_code'
    
    elif topic_name in ['order_event', 'order_detail']:
        return 'order_id'