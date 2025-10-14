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
    else:
        raise ValueError(f"Topic does not exsits: {topic_name}")