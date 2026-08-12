NYC Taxi 운행 데이터 스트리밍 파이프라인

1. 프로젝트 목표

NYC Yellow Taxi의 대용량 운행 기록을 실제 차량 운행 이벤트가 지속적으로 발생하는 상황처럼 재생하여, 데이터를 수집 → 처리 → 저장하는 스트리밍 데이터 파이프라인을 구축한다.

과거에 저장된 데이터를 단순 분석하는 것에서 끝나는 것이 아니라, 실제 모빌리티 서비스에서 다수의 차량 운행 데이터가 계속 들어오는 상황을 가정하여 실시간 데이터 처리 구조를 경험하는 것을 목표로 한다.

2. 프로젝트 선정 이유

모빌리티 서비스에서는 차량의 운행 정보가 지속적으로 발생하며, 이러한 데이터를 안정적으로 수집하고 처리할 수 있는 데이터 파이프라인이 필요하다.

이번 프로젝트를 통해 대용량 차량 운행 데이터가 어떤 흐름으로 수집되고 처리되는지 직접 구현해보고, Kafka, Spark, Airflow 등의 기술이 데이터 파이프라인 안에서 어떤 역할을 하는지 이해하고자 한다.

3. 사용할 데이터셋

NYC TLC Yellow Taxi Trip Record Data - 2025년 1월

* 출처: NYC Taxi & Limousine Commission
* 파일 형식: Parquet
* 데이터 크기: 3,475,226 rows × 20 columns

주요 컬럼 예시:

* tpep_pickup_datetime: 승차 시간
* tpep_dropoff_datetime: 하차 시간
* passenger_count: 승객 수
* trip_distance: 운행 거리
* PULocationID: 승차 지역 ID
* DOLocationID: 하차 지역 ID
* payment_type: 결제 방식
* fare_amount: 운임
* tip_amount: 팁
* total_amount: 총 결제 금액

데이터 출처: NYC TLC Trip Record Data

4. 데이터 파이프라인 초안

NYC Yellow Taxi Parquet 데이터
            ↓
Python Replay
과거 운행 데이터를 시간 순서대로 전송
            ↓
Kafka
차량 운행 이벤트 스트리밍 수집
            ↓
Spark
전처리 및 실시간/시간대별 집계
            ↓
PostgreSQL 또는 Parquet
처리 결과 저장
            ↓
분석 및 조회
Airflow
→ 전체 처리 작업의 순서, 스케줄, 재시도 등을 관리

5. 처리해보고 싶은 내용

초기에는 다음과 같은 기본 처리부터 구현할 예정이다.

* 승하차 시간 기준 데이터 정제
* 비정상적인 운행거리 또는 요금 데이터 확인
* 시간대별 운행 건수 집계
* 승차/하차 지역별 이용량 집계
* 평균 운행 거리 및 평균 운행 시간 계산

프로젝트 진행 상황에 따라 실시간 집계 및 이상 운행 데이터 탐지 기능도 확장해보고자 한다.

6. 사용해보고 싶은 기술 후보

* Python
* Pandas
* Apache Kafka
* Apache Spark
* Apache Airflow
* PostgreSQL 또는 Parquet

기술 스택은 프로젝트 진행 과정에서 데이터 특성과 구현 난이도에 따라 변경할 수 있다.

7. 현재 진행 상황

* NYC Yellow Taxi 2025년 1월 데이터 다운로드 완료
* Parquet 파일 Python/Pandas 로딩 완료
* 데이터 크기 및 컬럼 구조 확인 완료

## 데이터 출처

- NYC Taxi & Limousine Commission (TLC) Trip Record Data
- 사용 데이터: 2025년 1월 Yellow Taxi Trip Records
- 데이터 형식: Parquet
- 출처: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page