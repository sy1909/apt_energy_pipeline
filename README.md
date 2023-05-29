# 개인 데이터 엔지니어 파이프라인(data engineer pipeline) 학습

## 프로젝트 내용

특정 아파트 단지에는 840 세대가 있고 11개의 동으로 1동부터 11동까지 1-10까지는 한 층에 4세대 20층, 11동은 2세대씩 20층, 이렇게 840세대 존재
여기서 각 세대의 월별 전력 사용량, 난방 사용량, 수도 사용량, 각각의 탄소배출계수로 탄소배출량을 계산한 데이터를 데이터베이스에 저장 
기준치라고 가정한 값을 초과하면 slack 알림 봇을 통해 메세지 전송(이상탐지)
이후 예측기능 포함 예정

## 프로젝트 구성요소

1. 데이터 수집 장치(센서, IoT라 가정) -> 전력, 수도, 난방의 값을 계절 특성을 반영해서 랜덤하게 생성한다.
2. 데이터 포맷 및 ETL 프로세스 (Apache Kafka) 
3. 데이터베이스 (PostgreSQL)
4. 데이터 처리 및 분석 프레임워크 (Apache Spark streaming)
5. 데이터 분석 및 가시화 도구 (Grafana)

## 프로젝트 흐름

1. 시스템은 계절 특성을 반영한 랜덤으로 생성된 전력, 수도 및 난방 사용량 더미 데이터를 JSON 형식으로 Kafka를 통해 spark streaming으로 데이터를 전송합니다. 
2. 처리된 데이터는 PostgreSQL 데이터베이스에 저장되며, 필요에 따라 Apache Spark, TensorFlow 등의 데이터 처리 및 분석 프레임워크를 거칩니다.
3. 이 과정에서 기준치를 초과한 데이터의 경우 slack 알림봇을 통해 메세지 전송
4. 최종적으로 postgresQL의 데이터를 통해 Grafana를 사용하여 데이터를 분석하고 시각화

## 데이터베이스 테이블 구성

테이블:
1. `buildings` (건물 정보)
2. `households` (세대 정보)
3. `energy_usage` (에너지 사용량 정보)

`buildings` 테이블:
- `id` (SERIAL, PRIMARY KEY): 건물 고유 식별자
- `building_number` (INTEGER): 건물 번호

`households` 테이블:
- `id` (SERIAL, PRIMARY KEY): 세대 고유 식별자
- `building_id` (INTEGER): 건물 고유 식별자 (buildings 테이블 참조)
- `floor` (INTEGER): 층수
- `unit` (INTEGER): 세대 위치 (한 층 내 세대 순서)

`energy_usage` 테이블:
- `id` (SERIAL, PRIMARY KEY): 에너지 사용량 고유 식별자
- `household_id` (INTEGER): 세대 고유 식별자 (households 테이블 참조)
- `energy_type` (VARCHAR(10)): 에너지 종류 (전력, 난방, 수도)
- `usage_date` (DATE): 사용량에 대한 월별 날짜 (예: 2022-01-01)
- `amount` (DOUBLE PRECISION): 월별 사용량
- `emission` (DOUBLE PRECISION): 해당 사용량에 의한 탄소 배출량

이 테이블 및 컬럼 구성을 통해 단지의 각 세대 정보와 월별 전력 사용량, 난방 사용량, 수도 사용량 데이터를 관리하고, 각 사용량에 따른 탄소 배출량 계산도 가능. 
이 구조는 확장성이 있으며 새로운 에너지 사용량 및 세대 정보를 쉽게 추가하거나 갱신할 수 있다. 
또한, 다른 동이나 단지 정보를 추가하려면 `buildings` 및 `households` 테이블만 수정

## 결과

![image](https://github.com/sy1909/apt_energy_pipeline/assets/31126977/9f769e9e-229e-4d15-b0ee-ca19ca80859a)
데이터를 생성하고 kafka를 통해 토픽으로 전송

![image](https://github.com/sy1909/apt_energy_pipeline/assets/31126977/8b072774-3d1d-46ba-81ea-d9aedb4af0e9)
토픽에서 데이터를 받아와 postgresql에 적재하고 메세지에 포함은 안됐지만 이상치 슬랙 알림 실행중

![image](https://github.com/sy1909/apt_energy_pipeline/assets/31126977/b1ea300e-91e5-4eda-a367-c685bcbdc458)
이상치에 따라 알림중








