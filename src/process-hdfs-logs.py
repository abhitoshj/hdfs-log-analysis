"""
This script processes the raw HDFS logs and converts them into structured logs using the Drain algorithm.

The structured logs are then saved as parquet files in HDFS.
"""

import csv
import logging
import os
import sys
import tempfile

from logparser.Drain import LogParser
from pyspark.sql import Row, SparkSession

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

spark = SparkSession.builder \
    .appName("HDFS to DataFrame Processing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

st = 0.5
depth = 4
log_format = '<Date> <Time>,<PID> <Level> <Component>: <Content>'
regex = [
    r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)'
]
regex = [
    r"BP-\d+-\d+\.\d+\.\d+\.\d+-\d+"
]

hdfs_directory = "hdfs://localhost:9050//user/raghavendra/hadooplogs/raw/"
file_list = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()) \
    .listStatus(sc._jvm.org.apache.hadoop.fs.Path(hdfs_directory))
file_paths = [file.getPath().toString() for file in file_list]

def process_file_train(iter):
    print("Processing partition ")
    rows = []
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_input_path = os.path.join(temp_dir, 'input.log')
            local_output_path = os.path.join(temp_dir, 'input.log_structured.csv')
            
            with open(local_input_path, 'w') as f:
                for line in iter:
                    f.write(line + '\n')

            parser = LogParser(log_format, indir=os.path.dirname(local_input_path), 
                               outdir=temp_dir, depth=depth, st=st, rex=regex)
            parser.parse(os.path.basename(local_input_path))

            with open(local_output_path, 'r') as f:
                structured_content = f.read()

            for line in structured_content.splitlines():
                csv_reader = csv.reader([line])
                for values in csv_reader:
                    if len(values) == 10:
                        rows.append(Row(*values))
                    else:
                        print(f"Unexpected number of values: {len(values)} for line: {line}")

    except Exception as e:
        print(f"Error processing filecontent : {e}")
    
    return rows

first_file_path = file_paths[0]
first_file_rdd = sc.textFile(first_file_path,1000)
processed_rdd = first_file_rdd.mapPartitions(process_file_train)
transformed_df = spark.createDataFrame(processed_rdd)

aggregated_df = transformed_df

for file_path in file_paths[1:]:
    raw_rdd = sc.textFile(file_path,1000)
    processed_rdd = raw_rdd.mapPartitions(process_file_train)
    transformed_df = spark.createDataFrame(processed_rdd)
    aggregated_df = aggregated_df.union(transformed_df)

aggregated_df.printSchema()
aggregated_df.show()
print(f'Length of the aggregated df is : {aggregated_df.count()}')

aggregated_df.write.mode("overwrite").parquet("hdfs://localhost:9050/user/raghavendra/hadooplogs/structured/")

spark.stop()