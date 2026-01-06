# Olist E-Commerce 배송 지연 및 불만 분석 프로젝트

## MVP 목표
| 목표 | 타겟 데이터 | 데이터 유형 | 프레임워크 |
| --- | --- | --- | --- |
| **배송 지연 모니터링** | `olist_orders_dataset.csv` | 실시간 | Kafka, Spark Streaming, FastAPI, Prometheus+Grafana |
| **고객 불만 분석** | `olist_order_reviews_dataset.csv` | 실시간 | Kafka, Spark Streaming, Hugging Face Transformers |