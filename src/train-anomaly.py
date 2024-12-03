import os
from mlutils import FeatureExtractorSpark, extract_blkid_machine_info, load_HDFS_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def evaluate_classifier(classifier, train_data, test_data):
    model = classifier.fit(train_data)

    print("Making predictions on the test data...")
    predictions = model.transform(test_data)

    print("Genrating model metrics...")
    binary_evaluator = BinaryClassificationEvaluator(labelCol="Label", rawPredictionCol="rawPrediction")
    roc_auc_evaluator = BinaryClassificationEvaluator(labelCol="Label", metricName="areaUnderROC")
    accuracy = binary_evaluator.evaluate(predictions)
    roc_auc = roc_auc_evaluator.evaluate(predictions)
    tp = predictions.filter((col("Label") == 1) & (col("prediction") == 1)).count()  # True Positive
    fp = predictions.filter((col("Label") == 0) & (col("prediction") == 1)).count()  # False Positive
    tn = predictions.filter((col("Label") == 0) & (col("prediction") == 0)).count()  # True Negative
    fn = predictions.filter((col("Label") == 1) & (col("prediction") == 0)).count()  # False Negative
    precision = tp / (tp + fp) if tp + fp != 0 else 0
    recall = tp / (tp + fn) if tp + fn != 0 else 0
    if precision + recall != 0:
        f1_score = 2 * (precision * recall) / (precision + recall)
    else:
        f1_score = 0
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1 Score: {f1_score}")
    print(f"ROC-AUC: {roc_auc:.4f}")

    return model

if __name__ == '__main__':
    spark = SparkSession.builder\
             .master("local")\
             .appName("Colab")\
             .config("spark.executor.memory", "8g")\
             .config("spark.network.timeout", "600s")\
             .config("spark.default.parallelism", "4")\
             .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("Creating HDFS Structured log dataframe...")
    hdfs_log_fp = os.path.join(os.path.join(os.getcwd(), "..", "data", "HDFS_100k.log_structured.csv"))
    hdfs_log_df = spark.read.csv(hdfs_log_fp,header=True, inferSchema=True)
    print("Total rows in DataFrame: ", hdfs_log_df.count())

    print("Creating anomaly label dataframe...")
    anomaly_fp = os.path.join(os.path.join(os.getcwd(), "..",  "data", "anomaly_label.csv"))
    anomaly_df = spark.read.csv(anomaly_fp,header=True, inferSchema=True)

    print("Extracting important columns from HDFS structured log dataframe...")
    hdfs_log_df = extract_blkid_machine_info(hdfs_log_df)

    print("creating train and test dataframes...")
    train_df, test_df = load_HDFS_spark(hdfs_log_df,
     anomaly_df,
     train_ratio=0.8,
     save_csv=False)

    print("running feature extraction...")
    feature_extractor = FeatureExtractorSpark()
    train_feat = feature_extractor.fit_transform(train_df, term_weighting='tf-idf', normalization='zero-mean')
    test_feat = feature_extractor.transform(test_df)

    assembler = VectorAssembler(inputCols=["features"], outputCol="assembled_features")
    train_data = assembler.transform(train_feat)
    test_data = assembler.transform(test_feat)

    print("=========== Linear Regression model ==================")
    lr = LogisticRegression(featuresCol="assembled_features", labelCol="Label")
    model = evaluate_classifier(lr, train_data, test_data)

    print("Saving model to HDFS...")
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.defaultFS", "hdfs://localhost:9050")
    hadoop_conf.set("dfs.client.use.datanode.hostname", "true")
    hdfs_path = "hdfs://localhost:9050/user/raghavendra/anomaly/hdfs_anomaly_model"
    model.write().overwrite().save(hdfs_path)

    spark.stop()