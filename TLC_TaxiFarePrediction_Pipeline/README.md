# Taxi Fare Prediction Pipeline

개인 프로젝트인 apt_energy_pipeline을 만들기 전에 데이터 엔지니어 학습 강의 실습 내용

## 프로젝트 내용


우버 데이터활용 파이프라인을 구축하고 TLC 데이터를 사용한 택시 요금 예측 모델을 구축하기 위한 Airflow DAG 프로세스를 구현하는 것

Spark SQL을 이용해 데이터를 분석후 전처리, Spark MLlib를 이용해 모델 학습 후 저장하는 배치 데이터 가공, 저장 파이프라인을 airflow DAG 설계 후 동작을 확인


## 파일

- **preprocess.py**: 

데이터 전처리 및 학습 및 테스트 데이터셋 분할을 수행하는 파일. 이 스크립트는 PySpark 및 Spark SQL을 사용하여 데이터를 필터링, 형태 변환 및 무작위 분할하고 학습 및 테스트 데이터셋을 저장.

- **tune_hyperparameter.py**: 

하이퍼파라미터 튜닝을 위한 파일. 이 스크립트는 CrossValidator 및 ParamGridBuilder를 사용하여 하이퍼파라미터를 튜닝하고 최적의 하이퍼파라미터를 결정. 다만 회귀모델이라 유의미한 성능향상이 있지는 않았음. 이렇게 DAG를 구성할 수 있다 정도 학습.

- **train_model.py**: 

모델 학습 및 저장을 수행하는 파일. 이 스크립트는 전처리된 데이터와 튜닝된 하이퍼파라미터를 사용하여 회귀 모델을 학습하고 테스트 데이터셋으로 결과를 검증. 이후 최종 모델을 저장.


## Airflow

### DAG Workflow

1. 데이터 전처리(preprocess.py 실행)
2. 하이퍼 파라미터 튜닝(tune_hyperparameter.py 실행)
3. 모델 학습(train_model.py 실행)

## 결과






## Kafka


### 가상의 결제 정보를 생성하고 간단한 비정상 탐지 알림봇 실습


## 파일


- **payment_producer.py**:

가상의 결제 데이터를 생성하고 Kafka에 "payments" 토픽으로 전송하는 역할. 데이터는 결제 유형, 금액, 수령인과 같은 속성을 포함하며, 일정 시간 간격으로 생성됨.

- **fraud_detector.py**:

payment_producer로부터 받은 데이터를 분석해 비정상 결제 여부를 판별하는 역할. 비정상 결제로 판단되면 "fraud_payments" 토픽으로, 그렇지 않으면 "legit_payments" 토픽으로 Kafka에 전송됨.

- **fraud_processor.py**:

"fraud_payments" 토픽으로부터 메시지를 받아 처리하는 역할. 이상 결제로 판단된 데이터는 Slack 채널로 메시지가 전송되어 사용자가 알림을 받을 수 있음.

- **legit_processor.py**:

"legit_payments" 토픽으로부터 메시지를 받아 처리하는 역할. 정상 결제 데이터를 가진 메시지는 결제 유형과 금액, 수령인 등의 정보를 출력.



## 결과


