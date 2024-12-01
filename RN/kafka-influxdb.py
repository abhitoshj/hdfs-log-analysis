import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from datetime import datetime

# Set up Kafka Consumer
consumer = KafkaConsumer(
    'streaming_metrics_1',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grafana_group'
)

# Set up InfluxDB Client
influx_client = InfluxDBClient(url='http://localhost:8086', token='your_token', org='your_org')
bucket = 'streaming_metrics'  # InfluxDB bucket name

# Create an InfluxDB write API
write_api = influx_client.write_api()

# Consume messages from Kafka and write to InfluxDB
for message in consumer:
    # Parse the Kafka message value (JSON data)
    message_value = message.value.decode('utf-8')  # decode bytes to string
    json_data = json.loads(message_value)  # Convert JSON string to Python dictionary
    
    # Prepare the data point for InfluxDB
    point = Point("metrics") \
        .tag("level", json_data["Level"]) \
        .field("event_count", json_data["event_count"]) \
        .time(datetime.strptime(json_data["window_end"], "%Y-%m-%dT%H:%M:%S.%f%z"))  # timestamp to InfluxDB datetime
    
    # Write to InfluxDB
    write_api.write(bucket=bucket, record=point)
    print(f"Written to InfluxDB: {json_data}")

# Close the InfluxDB client connection when done
write_api.__del__()
