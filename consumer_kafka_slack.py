import os
from dotenv import load_dotenv

load_dotenv()

#kafka_bootstrap_servers = os.getenv("KAFKA_BROKERS")
topic = 'energy-data-topic'
postgres_host = os.getenv("POSTGRES_HOST")
postgres_db = os.getenv("POSTGRES_DB")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")
slack_api_token = os.getenv("SLACK_API_TOKEN")



from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, BooleanType
import pyspark.sql.functions as F
import psycopg2
from pyspark.sql.functions import udf

from slack.errors import SlackApiError
from slack import WebClient
import os

# DataFrame Schema 정의
schema = StructType([
    StructField("household_id", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("power", DoubleType(), True),
    StructField("water", DoubleType(), True),
    StructField("heating", DoubleType(), True),
])

# 값을 postgresql에 저장
def save_to_postgresql(data):
    conn = psycopg2.connect(host=postgres_host, database=postgres_db, user=postgres_user, password=postgres_password)
    cursor = conn.cursor()
    date = f"{data['year']}-{data['month']}-01"
    queries = ["INSERT INTO energy_usage_2 (household_id, energy_type, usage_date, amount, emission) VALUES (%s, %s, %s, %s, %s)"]
    energy_types = ['전력', '수도', '난방']
    rates = [0.4747, 0.449, 0.202]
    
    for query, energy_type, rate, key in zip(queries*3, energy_types, rates, ["power", "water", "heating"]):
        carbon_emission = float(data[key]) * rate
        cursor.execute(query, (data['household_id'], energy_type, date, float(data[key]), carbon_emission))

    conn.commit()
    cursor.close()
    conn.close()

    print("Insert successful!")

# kafka pyspark를 위한 설정
spark_version = '3.3.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:2.8.1 pyspark-shell'

kafka_bootstrap_servers = ",".join(["localhost:9091", "localhost:9092", "localhost:9093"])
topic = 'energy-data-topic'

spark = SparkSession.builder.appName("PySparkShell").getOrCreate()
kafka_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("startingOffsets", "earliest").option("failOnDataLoss", "False").option("subscribe", topic).load()
kafka_stream = kafka_stream.selectExpr("CAST(value AS STRING) as value")
kafka_stream = kafka_stream.select(F.from_json(col("value"), schema).alias("data")).select("data.*")

# Slack API 설정
slack_token = slack_api_token
slack_client = WebClient(slack_token)

def send_slack_notification(household_id, power, heating):
    message = f"주의! {household_id}번 가구에서 전력 사용량({power}) 또는 난방 사용량({heating})이 기준치를 초과했습니다."
    try:
        response = slack_client.chat_postMessage(
            channel='#비정상-알림',
            text=message
        )
        print(f"Message sent to Slack: {message}")
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e}")

def check_energy_usage(heating, power):
    return heating >= 1200 or power >= 500

udf_check_energy_usage = udf(lambda z, y: check_energy_usage(z, y), BooleanType())
kafka_stream = kafka_stream.withColumn('energy_alert', udf_check_energy_usage(col('heating'), col('power')))

def process_row(row):
    save_to_postgresql(row.asDict())
    if row.energy_alert:
        send_slack_notification(row.household_id, row.power, row.heating)

query = kafka_stream.writeStream.outputMode("append").foreach(process_row).start()
query.awaitTermination()
