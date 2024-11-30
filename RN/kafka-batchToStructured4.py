from pyspark.sql import SparkSession
import tempfile
import os
import csv
from pyspark.sql import Row
from logparser.Drain import LogParser
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_json, struct, col, from_json, to_timestamp, concat, lit, current_timestamp

from pyspark.sql.functions import window, count


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka to Structured DF") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# LogParser Config
st = 0.5
depth = 4
log_format = '<Date> <Time>,<PID> <Level> <Component>: <Content>'
regex = [
    r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)',  # IP
    r"BP-\d+-\d+\.\d+\.\d+\.\d+-\d+"           # HDFS Block ID
]

# HDFS Config
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

hdfs_directory = "hdfs://localhost:9050/user/raghavendra/hadooplogs/streaming/structured"

# Kafka Source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_topic") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

# Extract raw log lines
raw_logs_df = kafka_df.selectExpr("CAST(value AS STRING) AS log_message")

# Custom function to process a partition
def process_partition(iter_rows):
    rows = []
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_input_path = os.path.join(temp_dir, 'input.log')
            local_output_path = os.path.join(temp_dir, 'input.log_structured.csv')

            # Write logs to temp input file
            with open(local_input_path, 'w') as f:
                for line in iter_rows:
                    f.write(line['log_message'] + '\n')

            # Parse logs using LogParser
            parser = LogParser(log_format, indir=os.path.dirname(local_input_path), 
                               outdir=temp_dir, depth=depth, st=st, rex=regex)
            parser.parse(os.path.basename(local_input_path))
            # Read structured output and convert to rows
            with open(local_output_path, 'r') as f:
                csv_reader = csv.reader(f)
                next(csv_reader, None)
                for values in csv_reader:
                    if len(values) == 10:  # Adjust based on expected number of columns
                        #print(*values)
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
    print(f'processing batch {batch_id}')
    # Convert DataFrame to RDD and apply mapPartitions
    rows = batch_df.selectExpr("CAST(log_message AS STRING)").rdd.mapPartitions(process_partition).collect()
    if rows:
        processed_df = spark.createDataFrame(rows, schema)
         # Convert the DataFrame to JSON for writing to Kafka
        kafka_df = processed_df.select(to_json(struct([col(c) for c in processed_df.columns])).alias("value"))
        
        # Write the JSON DataFrame to Kafka
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "structured_log_topic") \
            .save()

# Write Stream with foreachBatch
query1 = raw_logs_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

# Step 3: Read from Kafka Stream
structured_kafka_df1 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "structured_log_topic") \
    .option("minOffsetsPerTrigger", "1000000") \
    .load()

structured_kafka_df2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "structured_log_topic") \
    .load()

# Extract JSON value from Kafka topic
parsed_logs_df1 = structured_kafka_df1.selectExpr("CAST(value AS STRING) as json_value")
parsed_logs_df2 = structured_kafka_df2.selectExpr("CAST(value AS STRING) as json_value")

# Parse JSON into columns
structured_df1 = parsed_logs_df1.select(from_json(col("json_value"), schema).alias("structured")).select("structured.*")
structured_df2 = parsed_logs_df2.select(from_json(col("json_value"), schema).alias("structured")).select("structured.*")

def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    # Write to HDFS
    batch_df.write \
        .format("parquet") \
        .mode("append") \
        .option("path", hdfs_directory) \
        .option("checkpointLocation", "/tmp/new3-checkpoint-structured-hdfs") \
        .save()
    
# Use the function in foreachBatch
hdfs_query = structured_df1.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

structured_df2 = structured_df2.withColumn("processing_time", current_timestamp())

structured_df2 = structured_df2.withColumn(
    "timestamp",
    to_timestamp(
        concat(
            lit("20"),  # Add the century prefix to the date
            col("Date"),
            lit(" "),
            col("Time")
        ),
        "yyyyMMdd HHmmss"  # Format to interpret the combined string
    )
)

# Perform windowed aggregation based on the current timestamp
aggregated_windowed_df = structured_df2 \
    .groupBy(
        window(col("processing_time"), "1 hour"),  # 1-hour tumbling window
        col("Level")
    ) \
    .agg(count("*").alias("event_count")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("Level"),
        col("event_count")
    )

# Serialize the output as JSON for Kafka
aggregated_windowed_df_json = aggregated_windowed_df.select(
    to_json(
        struct(
            col("window_start"),
            col("window_end"),
            col("Level"),
            col("event_count")
        )
    ).alias("value")
)

# Write the aggregated data to Kafka
query = aggregated_windowed_df_json.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "streaming_metrics") \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint-streaming-metrics-hourly") \
    .start()

query.awaitTermination()


# aggregated_df = structured_df2 \
#     .withWatermark("timestamp", "10 minutes") \
#     .groupBy(window(col("timestamp"), "1 minute"), col("Level")) \
#     .agg(count("*").alias("event_count"))

# # aggregated_df = structured_df2 \
# #     .groupBy("Level") \
# #     .agg(count("*").alias("event_count"))

# aggQuery = aggregated_df.writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .start()

# anotherQuery = aggregated_df.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "streaming_metrics") \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/new4-checkpoint-streaming-metrics") \
#     .start()

query1.awaitTermination()
hdfs_query.awaitTermination()
query.awaitTermination()
#anotherQuery.awaitTermination()