from service.init import *
from service.producer.topic import *
from service.producer.publish import *

if __name__=="__main__":
    delete_topics(Topic())
    create_topics(Topic())
    simulate_stream()