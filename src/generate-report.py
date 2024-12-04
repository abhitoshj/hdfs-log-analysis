"""
This script generates various reports from structured Hadoop logs stored in a Parquet file.

It generates and saves the following reports:
    - Log Severity Report: Aggregates counts by log level (ERROR, WARN, INFO, FATAL).
    - Exceptions Report: Counts occurrences of specific exceptions and orders them by date.
    - Write Latency Report: Calculates the average write latency for slow block receiver events.
    - Speed Report: Calculates the transfer speed for HDFS write operations.
    - Retry Count Report: Counts the number of retries for server connections and orders them by count.
    - Monthly Severity Report: Groups number of log entries by log level and by months, counts distinct event ids.

Each report is saved to both HDFS and a local file system in CSV format.
"""

import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    mean, col, cast, regexp_extract, split, regexp_replace, format_number, desc
)
from pyspark.sql.types import IntegerType


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

parquet_logfile = "hdfs://localhost:9050/user/raghavendra/hadooplogs/structured"
hdfs_out_path_root = "hdfs://localhost:9050/user/raghavendra/hadooplogs/report"
local_out_path_root = "file:///home/raghavendra/reports"

df = spark.read.parquet(parquet_logfile)

def create_logseverity_report():
    # Aggregate counts by log level
    log_levels = ['ERROR', 'WARN', 'INFO', 'FATAL']
    aggregated_df = df.groupBy().agg(
        *[F.count(F.when(df.Level == level, 1)).alias(level) for level in log_levels]
    )

    report_suffix = "severity"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    aggregated_df.write.mode("overwrite").csv(hdfs_out_path, mode='overwrite')

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    aggregated_df.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)


def create_exceptions_report():
    exceptions = df.where((F.col('EventTemplate') == 'IOException in offerService') | (F.col('EventTemplate') == 'java.io.FileNotFoundException: /opt/hdfs/data/current/VERSION (Permission denied)')).groupBy('date').count().orderBy('date', ascending=False)

    report_suffix = "exceptions"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    exceptions.write.mode("overwrite").csv(hdfs_out_path, mode='overwrite')

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    exceptions.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)

def create_writelatency_report():
    slowness_time = regexp_extract(df['ParameterList'], '(\d+)', 1).cast(IntegerType())
    average_write_latency = df.select(slowness_time.alias("slowness_time")) \
    .where(df['EventTemplate'] == 'Slow BlockReceiver write data to disk <*> (threshold=300ms)') \
    .select(mean("slowness_time").alias("average_write_latency"))

    report_suffix = "slowness"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    average_write_latency.write.mode("overwrite").csv(hdfs_out_path, mode='overwrite')

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    average_write_latency.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)

def create_speed_report():
    logtext = "src: <*>, dest: <*>, bytes: <*>, op: HDFS_WRITE, cliID: <*> offset: <*>, srvID: 4f8dd80e-ab80-41ad-b045-99cfeb1828d2, blockid: BP-<*>-<*><*>:<*>_<*>, duration: <*>"
    transferlog = df.select('ParameterList').where(df['EventTemplate'] == logtext)
    transferlog = transferlog.withColumn("cleaned_string", regexp_replace(col("ParameterList"), r"[\\[\\]']", ""))

    transferlog = transferlog.withColumn("split_values", split(col("cleaned_string"), ", ")) \
        .withColumn("src", col("split_values").getItem(0)) \
        .withColumn("dest", col("split_values").getItem(1)) \
        .withColumn("size", col("split_values").getItem(2)) \
        .withColumn("op", col("split_values").getItem(3)) \
        .withColumn("offset", col("split_values").getItem(4)) \
        .withColumn("blockid", col("split_values").getItem(5)) \
        .withColumn("blockid1", col("split_values").getItem(6)) \
        .withColumn("clientid", col("split_values").getItem(7)) \
        .withColumn("duration", col("split_values").getItem(8))

    transferlog = transferlog.withColumn("src", regexp_replace(col("src"), r"\[", ""))
    transferlog = transferlog.withColumn("src", regexp_replace(col("src"), r"\/", ""))
    transferlog = transferlog.withColumn("dest", regexp_replace(col("dest"), r"\/", ""))

    transferlog = transferlog.withColumn("duration", regexp_replace(col("duration"), r"\]", ""))
    transferlog = transferlog.withColumn("duration_s", col("duration") / 1000000000)
    transferlog = transferlog.withColumn("size_mb", col("size") / (1024 * 1024))

    transferlog = transferlog.withColumn("speed", format_number(col("size_mb") / col("duration_s"), 2))

    result_data = transferlog.select("src", "dest", "speed")
    result_data.show()

    report_suffix = "slowness"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    result_data.write.mode("overwrite").csv(hdfs_out_path, mode='overwrite')

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    result_data.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)

def create_retrycount_report():
    logtext = "Retrying connect to server: mesos-master-<*><*>. Already tried <*> time(s); retry policy is RetryUpToMaximumCountWithFixedSleep(maxRetries=<*>, sleepTime=<*> MILLISECONDS)"
    retry = df.select('ParameterList').where(df['EventTemplate'] == logtext)
    retry = retry.withColumn("retrycount", split(col("ParameterList"), ",").getItem(1))
    retry_result = retry.groupBy("retrycount").count().orderBy(col("count").desc())
    retry_result.show()

    report_suffix = "retrycount"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    retry_result.write.mode("overwrite").csv(hdfs_out_path, header=True)

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    retry_result.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)

def create_monthly_report(df):
    cdf = df.withColumn('Month-Year', F.date_format(F.to_date(col('Date'), 'yyyy-MM-dd'), 'yyyyMM'))
    grouped_df = cdf.groupBy('Level', 'Month-Year').agg(
        F.count('EventId').alias('total_records'),
        F.countDistinct('EventId').alias('distinct_eventid')
    ).orderBy('Level', desc('distinct_eventid'), desc('total_records'))

    print("Grouped and Sorted Record Counts:")
    report_suffix = "groupedByMonths"
    hdfs_out_path = f"{hdfs_out_path_root}/{report_suffix}"
    grouped_df.write.mode("overwrite").csv(hdfs_out_path, header=True)

    local_file_out = f"{local_out_path_root}/{report_suffix}.csv"
    grouped_df.coalesce(1).write.mode("overwrite").csv(local_file_out, header=True)


create_exceptions_report()
create_logseverity_report()
create_writelatency_report()  
create_speed_report()
create_retrycount_report()
create_monthly_report(df)

spark.stop()