from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import tempfile
import csv
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StructuredLogsQuery") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext

# Set Hadoop configuration
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

parquet_logfile = "hdfs://localhost:9050/user/raghavendra/hadooplogs/structured/structured_logs.parquet"
parquet_logfile = "hdfs://localhost:9050/user/raghavendra/hadooplogs/structured"

df = spark.read.parquet(parquet_logfile)
df.printSchema()
df.show()

print(f'Length of the aggregated df is : {df.count()}')
print(f"Error Count = {(df.filter(df.Level == 'ERROR')).count()}")

print(f"Count of unique templates {df.select('EventTemplate').distinct().count()}")

# Stop the Spark session
spark.stop()