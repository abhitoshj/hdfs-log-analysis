from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


# Initialize Spark session
spark = SparkSession.builder.appName("KafkaToHDFS").getOrCreate()

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "log_topic") \
    .load()

# Extract message value (raw logs)
raw_logs_df = kafka_df.selectExpr("CAST(value AS STRING)").alias("log_message")

# Define HDFS output location
# Set Hadoop configuration
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

hdfs_directory = "hdfs://localhost:9050//user/raghavendra/hadooplogs/streaming"

# Write raw logs to HDFS in batches once the data reaches a certain size
raw_logs_query = raw_logs_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .option("path", hdfs_directory) \
    .trigger(processingTime="10 seconds") \
    .start()

raw_logs_query.awaitTermination()
