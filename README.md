# Real-time E-commerce Delivery & Sales Dashboard

- **Olist E-commerce** 데이터셋을 활용하여 브라질 전자상거래 시장의 배송 현황과 판매 성과를 분석하는 데이터 파이프라인 프로젝트

- 상세 설명: https://jmhwang-dev.github.io/posts/ecommerce/

## Project Overview

* **Background:** 판매 중개 수수료가 핵심 수익원인 플랫폼 특성상, 판매자의 매출 성장이 곧 플랫폼의 성장을 의미합니다.
* **Problem:** LLM 기반 리뷰 분석 결과, 부정적인 고객 경험과 매출 저해의 핵심 원인이 '배송 지연'임을 식별했습니다.
* **Goal:** 실시간으로 배송 지연을 감지하고, 상품 등급에 따른 우선순위별 조치를 지원하여 운영 효율을 극대화합니다.
* **Approach:** **Lambda Architecture**를 도입하여 운영 이슈 대응을 위한 실시간성과 정밀한 분석을 위한 신뢰성을 동시에 확보했습니다.

## Tech Stack

| Category | Technology | Version | Description |
| :--- | :--- | :--- | :--- |
| **Ingestion** | **Apache Kafka** | 3.9.1 | Message Broker (KRaft Mode Cluster) |
| **Ingestion** | **Confluent** | 7.6.1 | Schema Registry (Avro Serialization) |
| **Processing** | **Apache Spark** | 3.5.6 | Structured Streaming & Batch Processing |
| **Storage** | **Apache Iceberg** | 1.9.1 | Table Format (ACID Transactions) |
| **Storage** | **MinIO** | RELEASE.2025 | S3 Compatible Object Storage |
| **Orchestration** | **Apache Airflow** | 3.0.6 | Workflow Management (DAGs) |
| **Monitoring** | **Prometheus** | 3.5.0 | Metric Collection (JMX Exporter) |
| **Monitoring** | **Grafana** | 12.2.0 | Pipeline Observability & Visualization |
| **BI** | **Apache Superset** | 5.0.0 | Interactive Business Dashboard |

## System Architecture

### 1. Logical Architecture (Lambda)
데이터 처리의 목적에 따라 두 가지 레이어를 결합하여 설계했습니다.

![Lambda](./docs/assets/img/portfolio/pipeline_logical.png)

* **Speed Layer (Real-time)**
    * **Components:** Kafka, Spark Structured Streaming
    * **Role:** 주문 상태 변경 이벤트를 실시간으로 스트리밍하여 배송 지연 여부를 즉각적으로 감지합니다.
* **Batch Layer (Historical)**
    * **Components:** Spark, Iceberg
    * **Role:** 대용량 이력 데이터를 집계하여 상품별 비즈니스 가치(매출 기여도, 인기)를 평가하는 지표를 생성합니다.
* **Serving Layer (Integrated)**
    * **Components:** Superset
    * **Role:** 실시간 배송 상태와 배치 기반의 분석 지표를 통합 시각화합니다.

### 2. Data Flow (Medallion)
데이터 품질 및 정제 수준에 따라 3단계 계층으로 파이프라인을 구성했습니다.

![Medallion](./docs/assets/img/portfolio/medallion.png)

* **Bronze:** Raw Data (Append-only CDC Logs)
* **Silver:** Cleaned & Augmented Data (Schema Validated)
* **Gold:** Aggregated Business Metrics (Sales, Lead Time)

## Key Engineering Highlights

### 파이프라인 및 연산 최적화
- Iceberg 스냅샷 기반 증분 처리를 통해 I/O 및 네트워크 병목 제거
- Spark Structured Streaming foreachBatch 단일 쿼리 패턴으로 컨텍스트 스위칭 비용 절감
- 제한된 GPU 환경에서 멀티프로세싱 및 동적 배칭 적용으로 LLM 추론 성능 향상

### 데이터 정합성 및 시스템 안정성
- Airflow 의존성을 최소화한 스냅샷 기반 멱등 파이프라인 구축
- Schema Registry 및 Avro 유니온 타입 기반의 유연한 스키마 설계로 변화 대응
- 실제 트래픽 패턴을 반영한 부하 시뮬레이션 환경 구축으로 개발 효율성 향상
- KRaft 기반 Kafka 3-노드 클러스터 구성 및 Schema Registry 연동으로 메시지 정합성 강화

### 저비용 고효율 아키텍처
- 온프레미스 환경에서 람다 아키텍처 구현 및 이기종 하드웨어 자원 역할 분리
- Prometheus, Grafana 기반 스트림 파이프라인 모니터링 환경 구축으로 실시간 처리 성능 추적

## Result (Dashboard & Monitoring)

### Sales & Delivery Dashboard (Superset)
![Sales Dashboard](./docs/assets/img/portfolio/sales.png)
_Sales Detail: 월별 판매 추이 및 상품별 매출 순위 시각화_


![Delivery Monitor](./docs/assets/img/portfolio/monitor.png)
_Delivery Monitor: 주문 단계별 리드타임 및 지연 현황 모니터링_

### Pipeline Monitoring (Grafana)
* **Metrics:** Kafka MessagesInPerSec, Spark Streaming Latency/InputRate 등

![Grafana Dashboard](./docs/assets/img/portfolio/grafana.png)
_Observability: 데이터 파이프라인의 병목 현상 및 지연을 실시간 추적_