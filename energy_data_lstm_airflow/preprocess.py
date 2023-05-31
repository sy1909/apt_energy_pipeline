from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import datetime
import functools

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Energy Data Processing") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()

data_path = 'your-data-directory-path'

# CSV 파일 읽기
CER = spark.read.csv(f'{data_path}/CER.csv', inferSchema=True, header=True)
natural_gas = spark.read.csv(f'{data_path}/natural_gas.csv', inferSchema=True, header=True)
coal = spark.read.csv(f'{data_path}/Newcastle_Coal.csv', inferSchema=True, header=True)
wti = spark.read.csv(f'{data_path}/WTI.csv', inferSchema=True, header=True)

new_columns = ['date', 'close', 'open', 'high', 'low', 'volume', 'return']
CER = CER.toDF(*new_columns)
natural_gas = natural_gas.toDF(*new_columns)
coal = coal.toDF(*new_columns)
wti = wti.toDF(*new_columns)

#CER.show()

# 날짜 변환 함수
def convert_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y- %m- %d")
    except ValueError:
        return datetime.strptime(date_str, "%b %d, %Y")

convert_date_udf = F.udf(lambda z: convert_date(z), DateType())

# 날짜 변환 적용
CER = CER.withColumn('date', convert_date_udf(F.col('date')))
natural_gas = natural_gas.withColumn('date', convert_date_udf(F.col('date')))
coal = coal.withColumn('date', convert_date_udf(F.col('date')))
wti = wti.withColumn('date', convert_date_udf(F.col('date')))

# 컬럼 선택 및 이름 변경
CER = CER.select('date', F.col('close').alias('CER'))
natural_gas = natural_gas.select('date', F.col('close').alias('natural_gas'))
coal = coal.select('date', F.col('close').alias('coal'))
wti = wti.select('date', F.col('close').alias('wti'))

# 데이터프레임 병합
dfs = [CER, natural_gas, coal, wti]
data = functools.reduce(lambda df1, df2: df1.join(df2, on=['date'], how='inner'), dfs)
data = data.sort('date').cache()

# 결과 표시
#data.show()

# 데이터 저장
data_dir = "your-airflow-data-directory-path"
data.write.format("parquet").mode('overwrite').save(f"{data_dir}/airflow_data/")