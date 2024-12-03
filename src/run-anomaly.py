import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
from mlutils import FeatureExtractorSpark, extract_blkid_machine_info, load_HDFS_spark

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')

spark = SparkSession.builder \
    .appName("StructuredLogsQuery") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

sc = spark.sparkContext

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

parquet_logfile = "hdfs://localhost:9050/user/raghavendra/hadooplogs/structured/structured_logs.parquet"
parquet_logfile = "hdfs://localhost:9050/user/raghavendra/hadooplogs/structured"

print(f"Reading parquet file from {parquet_logfile} ...")
df = spark.read.parquet(parquet_logfile)

print("Extracting block id and machine info ...")
hdfs_df = extract_blkid_machine_info(df)

print("Enhance data ...")
hdfs_df_1, hdfs_df_2 = load_HDFS_spark(hdfs_df, window='session', train_ratio=0.9, split_type='sequential', save_csv=False, window_size=0)
hdfs_df = hdfs_df_1.union(hdfs_df_2)

print("Feature extraction ...")
feature_extractor = FeatureExtractorSpark()
hdfs_feat = feature_extractor.fit_transform(hdfs_df)

print("Assembling features ...")
assembler = VectorAssembler(inputCols=["features"], outputCol="assembled_features")
hdfs_data = assembler.transform(hdfs_feat)

print("Loading the model ...")
pyspark_model = "hdfs://localhost:9050/user/raghavendra/anomaly/hdfs_anomaly_model"
lr_model = LogisticRegressionModel.load(pyspark_model)

print("Predicting ...")
predictions = lr_model.transform(hdfs_data)

print("Displaying block ids with anomalies ...")
anomaly_blocks = predictions.select("BlockId", "prediction").filter(predictions.prediction == 1)

print(f"Count of anomaly blocks: {anomaly_blocks.count()} / {hdfs_df.count()}")

spark.stop()