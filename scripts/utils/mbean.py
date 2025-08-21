from jmxquery import JMXConnection, JMXQuery

# JMX 연결 (호스트에서 localhost:매핑포트 사용)
jmx = JMXConnection("service:jmx:rmi:///jndi/rmi://localhost:19999/jmxrmi")

# 토픽 기반 브로커 MBean 쿼리 (consumer/producer 구분 없이 토픽 메트릭스 확인)
query = JMXQuery("kafka.server:type=BrokerTopicMetrics,name=*,topic=*")  # 토픽별 메트릭스 (예: TotalProduceRequestsPerSec 등)
results = jmx.query([query])

# 결과 출력 (MBean 이름과 값)
for result in results:
    print(f"MBean: {result.mBeanName}, Attribute: {result.attribute}, Value: {result.value}")

# 특정 토픽 메트릭스 예시 (BytesInPerSec 등)
topic_query = JMXQuery("kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=stream_order_status")
topic_results = jmx.query([topic_query])
for res in topic_results:
    print(f"Metric for {res.mBeanName}: {res.value}")