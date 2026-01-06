# Olist E-Commerce 배송 지연 및 불만 분석 프로젝트

## SLA 요약

### 배송 지연 모니터링
| SLI | SLO | 데이터 소스 | 구현 |
|-----|-----|-------------|------|
| 처리 시간 | Bronze → Silver 처리 시간 < 10분 | `orders_raw` (Kafka Topic) | - Spark Streaming 구성<br>- FastAPI Producer |
| 시스템 가용성 | 시스템 업타임 > 99% (1일 다운타임 ≤ 14분) | `kafka_cluster` (Kafka Cluster Metrics) | - Prometheus + Grafana 패널<br>- Alertmanager 알람 |
| Kafka Lag | 오프셋 차이 < 10분 | `orders_raw` (Kafka Topic) | - Prometheus + Grafana 패널<br>- Alertmanager 알람 |
| 오류율 | 오류율 < 1% | `orders_dlq` (Dead Letter Queue) | - DLQ 재처리 스크립트<br>- Spark RocksDB checkpoint |

### 고객 불만 분석
- **SLA 개요**: 리뷰 텍스트의 실시간 분석으로 불만 패턴 도출, 목표 가용성 99% (NLP 지연 고려해 SLO 완화).

| SLI | SLO | 데이터 소스 | 구현 |
|-----|-----|-------------|------|
| 처리 시간 | Bronze → Silver 처리 시간 < 30분 (NLP 지연 완화) | `review_raw` (Kafka Topic) | - Spark Streaming 구성<br>- Hugging Face Transformers |
| 시스템 가용성 | 시스템 업타임 > 99% (1일 다운타임 ≤ 14분) | `kafka_cluster` (Kafka Cluster Metrics) | - Prometheus + Grafana 패널<br>- Alertmanager 알람 |
| Kafka Lag | 오프셋 차이 < 10분 | `review_raw` (Kafka Topic) | - Prometheus + Grafana 패널<br>- Alertmanager 알람 |
| 오류율 | 오류율 < 1% | `review_dlq` (Dead Letter Queue) | - DLQ 재처리 스크립트<br>- Spark RocksDB checkpoint |