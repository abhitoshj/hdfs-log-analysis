"""
This scripts processes the raw logs from Kafka, converts them into structured logs using the Drain algorithm, and then writes the structured logs back to Kafka.
"""

import csv
import os
import tempfile
import time
from logparser.Drain import LogParser
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_json, struct, col, from_json, to_timestamp, concat, lit, current_timestamp, window, count
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Kafka to Structured DF") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

system_metrics_topic = "logprocessing_system_metrics"

st = 0.5
depth = 4
log_format = '<Date> <Time>,<PID> <Level> <Component>: <Content>'
regex = [
    r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)',
    r"BP-\d+-\d+\.\d+\.\d+\.\d+-\d+"
]

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

hdfs_directory = "hdfs://localhost:9050/user/raghavendra/hadooplogs/streaming/structured"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_topic_1") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

raw_logs_df = kafka_df.selectExpr("CAST(value AS STRING) AS log_message")

def process_partition(iter_rows):
    rows = []
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_input_path = os.path.join(temp_dir, 'input.log')
            local_output_path = os.path.join(temp_dir, 'input.log_structured.csv')

            with open(local_input_path, 'w') as f:
                for line in iter_rows:
                    f.write(line['log_message'] + '\n')
            parser = LogParser(log_format, indir=os.path.dirname(local_input_path), 
                               outdir=temp_dir, depth=depth, st=st, rex=regex)
            parser.parse(os.path.basename(local_input_path))
            with open(local_output_path, 'r') as f:
                csv_reader = csv.reader(f)
                next(csv_reader, None)
                for values in csv_reader:
                    if len(values) == 10:
                        rows.append(Row(*values))
    except Exception as e:
        print(f"Error processing logs: {e}")
    return rows

schema = StructType([
    StructField("LineId", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("PID", StringType(), True),
    StructField("Level", StringType(), True),
    StructField("Component", StringType(), True),
    StructField("Content", StringType(), True),
    StructField("EventId", StringType(), True),
    StructField("EventTemplate", StringType(), True),
    StructField("ParameterList", StringType(), True),
])

def process_batch(batch_df, batch_id):
    start_time = time.time()
    record_count = batch_df.count()

    print(f'processing batch {batch_id}')
    rows = batch_df.selectExpr("CAST(log_message AS STRING)").rdd.mapPartitions(process_partition).collect()
    if rows:
        processed_df = spark.createDataFrame(rows, schema)
        kafka_df = processed_df.select(to_json(struct([col(c) for c in processed_df.columns])).alias("value"))
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "structured_log_topic") \
            .save()     
    end_time = time.time()
    latency = end_time - start_time
    throughput = record_count / latency if latency > 0 else 0
    metrics_df = spark.createDataFrame([
        Row(batch_id=str(batch_id), record_count=record_count, latency=latency, throughput=throughput, timestamp = start_time)
    ])
    metrics_df.select(to_json(struct([col(c) for c in metrics_df.columns])).alias("value")).write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", system_metrics_topic) \
        .save()

query1 = raw_logs_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

structured_kafka_df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "structured_log_topic") \
    .option("maxOffsetsPerTrigger", "1000000") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

structured_kafka_df2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("maxOffsetsPerTrigger", "10000") \
    .option("subscribe", "structured_log_topic") \
    .load()

parsed_logs_df1 = structured_kafka_df1.selectExpr("CAST(value AS STRING) as json_value")
parsed_logs_df2 = structured_kafka_df2.selectExpr("CAST(value AS STRING) as json_value")

structured_df1 = parsed_logs_df1.select(from_json(col("json_value"), schema).alias("structured")).select("structured.*")
structured_df2 = parsed_logs_df2.select(from_json(col("json_value"), schema).alias("structured")).select("structured.*")

def write_to_hdfs(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .option("path", hdfs_directory) \
        .option("checkpointLocation", "/tmp/new42-checkpoint-structured-hdfs") \
        .save()
    
hdfs_query = structured_df1.writeStream \
    .foreachBatch(write_to_hdfs) \
    .outputMode("append") \
    .trigger(processingTime="30 minutes")\
    .start()

structured_df2 = structured_df2.withColumn("processing_time", current_timestamp())

structured_df2 = structured_df2.withColumn(
    "timestamp",
    to_timestamp(
        concat(
            lit("20"), 
            col("Date"),
            lit(" "),
            col("Time")
        ),
        "yyyyMMdd HHmmss"
    )
)

aggregated_windowed_df = structured_df2 \
    .groupBy(
        window(col("processing_time"), "1 minute"),  # 1-minute tumbling window
        col("Level")
    ) \
    .agg(
        F.count("*").alias("event_count"),
        F.first("timestamp").alias("orig_first_timestamp"),
        F.last("timestamp").alias("orig_last_timestamp")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("Level"),
        col("event_count"),
        col("orig_first_timestamp"),
        col("orig_last_timestamp")
    )

aggregated_windowed_df_json = aggregated_windowed_df.select(
    to_json(
        struct(
            col("window_start"),
            col("window_end"),
            col("Level"),
            col("event_count"),
            col("orig_first_timestamp"),
            col("orig_last_timestamp")
        )
    ).alias("value")
)

query = aggregated_windowed_df_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "streaming_metrics_1") \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint3-streaming-metrics-hourly") \
    .trigger(processingTime="1 minute")\
    .start()

query.awaitTermination()
query1.awaitTermination()
hdfs_query.awaitTermination()
query.awaitTermination()