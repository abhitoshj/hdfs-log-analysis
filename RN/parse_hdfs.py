from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import tempfile
import csv
import os
from logparser.Drain import LogParser
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HDFS to DataFrame Processing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext

# Set Hadoop configuration
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

st = 0.5  # Similarity threshold
depth = 4  # Depth of all leaf nodes
log_format = '<Date> <Time>,<PID> <Level> <Component>: <Content>' # Define log format to split message fields
# Regular expression list for optional preprocessing (default: [])
regex = [
    r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)' # IP
]

regex = [
    r"BP-\d+-\d+\.\d+\.\d+\.\d+-\d+" # HDFS Block ID
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

            # Assuming the LogParser is still needed for other operations
            parser = LogParser(log_format, indir=os.path.dirname(local_input_path), 
                               outdir=temp_dir, depth=depth, st=st, rex=regex)
            parser.parse(os.path.basename(local_input_path))

            with open(local_output_path, 'r') as f:
                structured_content = f.read()

            # Use csv.reader to handle the parsing
            for line in structured_content.splitlines():
                # Use csv.reader to split line correctly
                csv_reader = csv.reader([line])  # Wrap line in a list to make it iterable
                for values in csv_reader:
                    if len(values) == 10:  # Adjust this to match your expected number of fields
                        rows.append(Row(*values))
                    else:
                        print(f"Unexpected number of values: {len(values)} for line: {line}")

    except Exception as e:
        print(f"Error processing filecontent : {e}")
    
    return rows

# Process the first file to infer the schema
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

# Stop the Spark session
spark.stop()