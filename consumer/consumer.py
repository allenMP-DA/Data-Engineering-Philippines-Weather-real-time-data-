import time
import json
import boto3
from kafka import KafkaConsumer

# Minio Client
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9002",
    aws_access_key_id="admin",
    aws_secret_access_key="password123",
)
bucket_name = "weather-update-bucket"l  

weather_consumer = KafkaConsumer(
    "weather-topic",
    bootstrap_servers=["host.docker.internal:29092"],
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    group_id="weather-update-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)
print("Consumer streaming and saving to Minio")
for message in weather_consumer:
    record = message.value
    place = record.get("city")
    ts = record.get("current", {}).get("time")
    key = f"{place}/{ts}.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record),
        ContentType="application/json",
    )
    print(f"Saved record for {place})= s3://{bucket_name}/{key}")
