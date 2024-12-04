import json
from datetime import datetime

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

KAFKA_TOPIC = "logprocessing_system_metrics"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "E4Ro5I-EGs-KDhFlED2LxHd1YV2BohmiAWyvjaZTVzcuoVa7oce5plfQ9J92iCSdby1MREPs4SD0Ows_ydEngg=="
INFLUXDB_ORG = "des"
INFLUXDB_BUCKET = "logprocessing_system_metrics"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id='system_metric_group',  
    auto_offset_reset='latest',
    enable_auto_commit=True,    
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

for message in consumer:
    metrics = message.value
    timestamp_iso = datetime.utcfromtimestamp(metrics["timestamp"]).isoformat() + "Z"
    point = Point("streaming_metrics") \
        .tag("batch_id", metrics["batch_id"]) \
        .field("record_count", metrics["record_count"]) \
        .field("latency", metrics["latency"]) \
        .field("throughput", metrics["throughput"]) \
        .time(timestamp_iso)
    write_api.write(bucket=INFLUXDB_BUCKET, record=point)
    print(f"Inserted metrics: {metrics}")