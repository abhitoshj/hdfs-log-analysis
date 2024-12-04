"""
This script reads log files from a specified directory and sends each line
as a message to a Kafka topic using a Kafka producer.

This is a utility that emulates a log producer that sends log messages to a Kafka topic.
"""

from kafka import KafkaProducer
import os
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

log_folder = '/home/raghavendra/logs/hadoop/raw'

for filename in os.listdir(log_folder):
    with open(os.path.join(log_folder, filename), 'r') as f:
        for line in f:
            producer.send('log_topic_1', value=line.encode('utf-8'))
            producer.flush()
            time.sleep(0.005)