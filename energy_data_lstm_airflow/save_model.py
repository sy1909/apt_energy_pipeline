from pyspark.sql import SparkSession
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml import Pipeline
from tensorflow.keras.models import load_model
from sklearn.model_selection import train_test_split
import numpy as np

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Energy Data Processing2") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()

# 데이터 불러오기
data_dir = "your-airflow-data-directory-path"
data = spark.read.format("parquet").load(f"{data_dir}/airflow_data/")

# Spark 데이터 프레임을 pandas 데이터 프레임으로 변환
data = data.toPandas()
#data.show()

import pandas as pd
import numpy as np
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense, LSTM, concatenate
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

# 요소들이 적절하게 조정되어 있는 데이터 프레임(data)가 있다고 가정합니다.

# 데이터 정규화를 위한 MinMaxScaler 적용
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data[['wti', 'natural_gas', 'coal', 'CER']])
scaled_wti = scaled_data[:, 0]
scaled_gas = scaled_data[:, 1]
scaled_coal = scaled_data[:, 2]
scaled_cer = scaled_data[:, 3]

# 입력 데이터 정의
input_wti = Input(shape=(1, 1), name='wti')
input_gas = Input(shape=(1, 1), name='natural_gas')
input_coal = Input(shape=(1, 1), name='coal')

# 각 입력에 대한 LSTM 레이어 정의
lstm_wti = LSTM(32)(input_wti)
lstm_gas = LSTM(32)(input_gas)
lstm_coal = LSTM(32)(input_coal)

# ---
# from tensorflow.keras.layers import Dropout

# # 추가: LSTM 레이어의 층을 늘림, Dropout 적용
# lstm_wti = LSTM(64, return_sequences=True)(input_wti)
# lstm_wti = Dropout(0.2)(lstm_wti)
# lstm_wti = LSTM(32)(lstm_wti)

# lstm_gas = LSTM(64, return_sequences=True)(input_gas)
# lstm_gas = Dropout(0.2)(lstm_gas)
# lstm_gas = LSTM(32)(lstm_gas)

# lstm_coal = LSTM(64, return_sequences=True)(input_coal)
# lstm_coal = Dropout(0.2)(lstm_coal)
# lstm_coal = LSTM(32)(lstm_coal)
# --- mse 0.0388로 비슷

# LSTM 레이어 병합
concatenated_lstm = concatenate([lstm_wti, lstm_gas, lstm_coal], name='concatenate')

# 출력 레이어 추가
output_layer = Dense(1, name='CER')(concatenated_lstm)

# 모델 구성
model = Model(inputs=[input_wti, input_gas, input_coal], outputs=output_layer)

# 모델 컴파일
model.compile(optimizer='adam', loss='mse')

# 입력 데이터를 형태(shape) (samples, 1, 1)로 재구성
wti_reshaped = scaled_wti.reshape((scaled_wti.shape[0], 1, 1))
gas_reshaped = scaled_gas.reshape((scaled_gas.shape[0], 1, 1))
coal_reshaped = scaled_coal.reshape((scaled_coal.shape[0], 1, 1))

# 데이터 분할
X_train, X_test, y_train, y_test = train_test_split(scaled_data, scaled_cer, test_size=0.2, random_state=42)

# 모델 훈련
model.fit(
    {'wti': X_train[:, 0].reshape((-1, 1, 1)),
     'natural_gas': X_train[:, 1].reshape((-1, 1, 1)),
     'coal': X_train[:, 2].reshape((-1, 1, 1))},
    y_train,
    epochs=200,
    batch_size=10
)

# 예측값 생성
predictions = model.predict(
    {'wti': X_test[:, 0].reshape((-1, 1, 1)),
     'natural_gas': X_test[:, 1].reshape((-1, 1, 1)),
     'coal': X_test[:, 2].reshape((-1, 1, 1))})

# 성능 평가 (MSE)
mse = mean_squared_error(y_test, predictions)
print(f"Mean Squared Error: {mse}")

# 모델 저장
model.save('your-model-save-path/my_model.h5')