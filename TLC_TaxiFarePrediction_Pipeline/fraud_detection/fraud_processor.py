from kafka import KafkaConsumer
import json

FRAUD_TOPIC = "fraud_payments"
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=brokers)

from slack.errors import SlackApiError
from slack import WebClient

# Slack API 설정
slack_token =  '{SLACK_TOKEN}' 

# Slack WebClient 초기화
slack_client = WebClient(slack_token)

for message in consumer:
    # Slack으로 메시지 전송
    response = slack_client.chat_postMessage(
        channel='-', # 이거 아님
        text=message.value.decode('utf-8')  # 수신한 메시지
    )
    print(f"Message sent to Slack: {message.value.decode()}")
