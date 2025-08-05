# Olist E-Commerce 배송 지연 및 불만 분석 프로젝트

## MVP 목표
| 목표 | 타겟 데이터 | 데이터 유형 | 프레임워크 |
| --- | --- | --- | --- |
| **배송 지연 모니터링** | `olist_orders_dataset.csv` | 실시간 | Kafka, Spark Streaming, FastAPI, Prometheus+Grafana |
| **고객 불만 분석** | `olist_order_reviews_dataset.csv` | 실시간 | Kafka, Spark Streaming, Hugging Face Transformers |

## SLA 요약

### 배송 지연 모니터링
| SLI | SLO | 데이터 소스 |
| --- | --- | --- |
| 처리 시간 | Bronze → Silver 처리 시간 < 10분 | `orders_raw` (Kafka Topic) |
| 시스템 가용성 | 가용성 99% (1일 다운타임 ≤ 14분) | `kafka_cluster` (Metrics) |
| Kafka Lag | Lag < 10분 | `orders_raw` (Kafka Topic) |
| 오류율 | 오류율 < 1% | `orders_raw` (Kafka Topic) |

### 고객 불만 분석
| SLI | SLO | 데이터 소스 |
| --- | --- | --- |
| 처리 시간 | Bronze → Silver 처리 시간 < 10분 | `review_raw` (Kafka Topic) |
| 시스템 가용성 | 가용성 99% (1일 다운타임 ≤ 14분) | `kafka_cluster` (Metrics) |
| Kafka Lag | Lag < 10분 | `review_raw` (Kafka Topic) |
| 오류율 | 오류율 < 1% | `review_dlq` (Dead Letter Queue) |