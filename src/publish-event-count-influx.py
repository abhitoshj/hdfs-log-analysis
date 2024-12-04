"""
This script reads the event count from the Kafka topic and writes it to InfluxDB.
"""

import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from datetime import datetime

consumer = KafkaConsumer(
    'streaming_metrics_1',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='grafana_group'
)

influx_client = InfluxDBClient(url='http://localhost:8086', token='E4Ro5I-EGs-KDhFlED2LxHd1YV2BohmiAWyvjaZTVzcuoVa7oce5plfQ9J92iCSdby1MREPs4SD0Ows_ydEngg==', org='des')
bucket = 'des_event_count'

write_api = influx_client.write_api()

for message in consumer:
    message_value = message.value.decode('utf-8')
    json_data = json.loads(message_value)
    
    point = Point("metrics") \
        .tag("level", json_data["Level"]) \
        .field("event_count", json_data["event_count"]) \
        .time(datetime.strptime(json_data["window_end"], "%Y-%m-%dT%H:%M:%S.%f%z"))
    
    write_api.write(bucket=bucket, record=point)
    print(f"Written to InfluxDB: {json_data}")

write_api.__del__()