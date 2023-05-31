import pandas as pd
from tensorflow.keras.models import load_model
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from pyspark.sql import SparkSession


# SparkSession 생성
spark = SparkSession.builder \
    .appName("Energy Data Processing3") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()

# 데이터 불러오기
data_dir = "/your-data-path"
data = spark.read.format("parquet").load(f"{data_dir}/airflow_data/")

# Spark 데이터 프레임을 pandas 데이터 프레임으로 변환
data = data.toPandas()


# 저장된 모델 불러오기
loaded_model = load_model('your-model-save-path/my_model.h5')

# X_train, y_train 추출
X_train = data[['wti', 'natural_gas', 'coal']]
y_train = data['CER']

# Scalers 생성
input_scaler = MinMaxScaler()
cer_scaler = MinMaxScaler()

# Scalers 피팅
input_scaler.fit(X_train)
cer_scaler.fit(y_train.values.reshape(-1, 1))

# 사용자로부터 입력값 받기 , 지금은 test값 입력
example_wti = float(78.61)
example_gas = float(4.559)
example_coal = float(404.15)

# 입력값을 데이터 프레임에 추가하고 스케일링
example_values = pd.DataFrame(
    {'wti': [example_wti], 'natural_gas': [example_gas], 'coal': [example_coal]}
)
scaled_example_values = input_scaler.transform(example_values)

# 예측 수행
example_prediction = loaded_model.predict([scaled_example_values[:, 0].reshape(1, -1), 
                                           scaled_example_values[:, 1].reshape(1, -1), 
                                           scaled_example_values[:, 2].reshape(1, -1)])

# 예측한 스케일된 값을 원래 스케일로 변환
unscaled_example_cer = cer_scaler.inverse_transform(example_prediction)

print(f"The predicted CER for WTI={example_wti}, natural_gas={example_gas}, and coal={example_coal} is: {unscaled_example_cer[0][0]}")
