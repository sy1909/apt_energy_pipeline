# energy-pipeline airflow dag 학습

앞서 undertheC 프로젝트에서 lstm 적용 못했던 것을 적용하기 위해 학습 진행
https://github.com/sy1909/undertheC


apt-data-pipeline에도 airflow 적용 가능한지 학습 중

## 데이터 와 lstm 학습 진행
investing.com 에서 탄소배출권 , natural_gas , newastle_coal , wti 선물 가격 데이터를 바탕으로 lstm모델 학습을 진행한 파일
**lstm_predict.ipynb**


## 위 코드를 통해 lstm 예측을 확인하고 airflow를 통해 dag을 설계

### **전처리(preprocess) -> 모델 저장(save_model) -> 특정 값을 입력받아 예측(predict)**

airflow/dags/energy-pipeline.py
```python
from datetime import datetime
from airflow import DAG

default_args = {
  'start_date': datetime(2023, 1, 1),
}

with DAG(dag_id='energy-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['spark'],
         catchup=False) as dag:
    pass

    #생략
```
dag를 만드는 python 파일 airflow에 적용될려면 30초정도 걸린다. 위는 뼈대만 만들어 놓은 dag 스켈레톤

airflow webserver -p 8080
airfkiw scheduler 


## preprocess.py

Apache Spark를 사용하여 에너지 선물 가격 데이터를 처리. 

Pyspark을 사용하여 데이터 파일을 읽고, 처리, 통합 및 저장하는 과정을 거쳐 최종 데이터셋을 생성.


1. 필요한 라이브러리를 임포트하고, SparkSession을 생성.
2. 데이터 파일(CER, Natural_gas, Coal, WTI)을 읽어서 데이터프레임에 저장.
3. 각 데이터프레임의 컬럼 이름을 변경.
4. 날짜 데이터의 형식을 변환하는 함수를 정의하고, 이를 사용하여 모든 데이터프레임의 날짜를 변환.
5. 각 데이터프레임에서 'date'와 관련된 종가 데이터만 선택, 컬럼 이름을 변경.
6. 'date'를 기준으로 데이터프레임을 통합(join)하여 최종 데이터셋을 생성.
7. 결과를 parquet 파일로 저장.


## save_model.py

PySpark, Keras, scikit-learn 등 여러 라이브러리를 활용하고 있으며 전체 과정:


1. SparkSession을 생성하고 Parquet 형식의 데이터를 로드.
2. Spark 데이터 프레임을 pandas 데이터 프레임으로 변환.
3. 데이터에 MinMaxScaler를 적용하여 정규화를 수행.
4. 각 입력 변수에 대해 LSTM 레이어를 정의하고 병합.
5. 출력 레이어를 추가하여 모델을 구성하고 컴파일.
6. 훈련 및 테스트 데이터로 분할한 후 모델을 훈련.
7. 모델을 사용하여 예측값을 생성하고 성능을 평가. (MSE가 출력)
8. 훈련된 모델을 저장.

- 모델의 성능을 개선하려면 하이퍼파라미터를 조절(예: LSTM 뉴런 수 변경, 배치 크기 조정, 에포크 수 변경).
- 추가로 Dropout 레이어를 사용하여 과적합을 방지 가능(주석 처리된 예시 코드).


## predict.py

모델을 불러와 사용자 입력에 대한 에너지 가격을 예측.


1. 필요한 라이브러리를 임포트.
2. SparkSession을 생성.
3. 데이터를 로드하고, Spark 데이터 프레임을 pandas 데이터 프레임으로 변환.
4. 저장된 딥러닝 모델을 로드.
5. 훈련 데이터셋의 X_train(입력 변수)과 y_train(타겟 변수)를 추출.
6. 입력 변수와 타겟 변수를 정규화하기 위한 MinMaxScaler 객체를 생성, 훈련 데이터를 통해 스케일러를 피팅.
7. 사용자로부터 입력값 받음. (여기서는 테스트 용도로 입력값을 사용.)
8. 입력값을 데이터 프레임에 추가하고, 피팅된 스케일러를 이용해 정규화.
9. 정규화된 입력값을 불러온 딥러닝 모델에 입력하여 에너지 가격 예측을 수행.
10. 케일된 예측값을 원래 스케일로 변환하고 예측 결과를 출력.



## dag 실행도 전에 실패하는 에러 디버깅

로그에서 발견한 문제는 "unsupported pickle protocol: 5"와 관련된 오류. 

airflow sceduler , webserver 두개는 같은 python 환경에서 실행되어야 한다. 터미널과 vscode 파이썬 환경이 달라서 일어난 에러

이 오류는 일반적으로 Python 버전 차이로 인해 발생. 웹 서버, 스케줄러가 모두 동일한 Python 환경에서 실행되고 있는지 확인.










