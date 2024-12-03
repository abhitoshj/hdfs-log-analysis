import re
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, collect_list, rand, explode
from pyspark.sql.types import ArrayType, StringType, IntegerType, StructType, StructField
from pyspark.ml.feature import HashingTF, IDF, StandardScaler

def extract_blkid_machine_info(df):
    df.select(
        '*',
        F.regexp_extract('Content', r'(blk_-?\d+)', 0).alias('BlockID'),
        F.regexp_replace(F.regexp_extract('Content', r'src: (/[^ ]+)', 1), r'^/', '').alias('Source'),
        F.regexp_replace(F.regexp_extract('Content', r'dest: (/[^ ]+)', 1), r'^/', '').alias('Destination')
    ).select(
        '*',
        F.regexp_extract('Source', r'([^:]+)', 1).alias('SourceIP'),
        F.regexp_extract('Source', r'([^:]+):(\d+)', 2).alias('SourcePort'),
        F.regexp_extract('Destination', r'([^:]+)', 1).alias('DestinationIP'),
        F.regexp_extract('Destination', r'([^:]+):(\d+)', 2).alias('DestinationPort')
    )
    return df

class FeatureExtractorSpark:
    def __init__(self):
        self.hashingTF = None
        self.idf_model = None
        self.scaler_model = None
        self.normalization = None

    def fit_transform(self, df, term_weighting='tf-idf', normalization=None, oov=False, min_count=1):
        """
        Fit and transform the data using TF-IDF and normalization.

        Args:
            df: Spark DataFrame with columns `BlockId`, `EventSequence`, and `Label`.
            term_weighting: 'tf-idf' or None.
            normalization: 'zero-mean' or 'sigmoid'.
            oov: Not directly applicable in Spark ML.
            min_count: Minimum count for terms (use CountVectorizer if necessary).

        Returns:
            Transformed Spark DataFrame with features.
        """
        self.normalization = normalization

        print("Tokenizing the EventSequence if necessary...")
        tokenize_udf = udf(lambda x: x, ArrayType(StringType()))  # Assuming EventSequence is already tokenized
        df = df.withColumn("tokens", tokenize_udf(col("EventSequence")))

        print("Computing Term Frequency...")
        self.hashingTF = HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=2**16)
        tf_df = self.hashingTF.transform(df)

        print("Computing Inverse Document Frequency ...")
        if term_weighting == 'tf-idf':
            idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
            self.idf_model = idf.fit(tf_df)
            tfidf_df = self.idf_model.transform(tf_df)
            features_col = "tfidf_features"
        else:
            features_col = "raw_features"
            tfidf_df = tf_df

        # Step 3: Apply Normalization (if specified)
        if normalization == 'zero-mean':
            scaler = StandardScaler(inputCol=features_col, outputCol="scaled_features", withMean=True, withStd=False)
            self.scaler_model = scaler.fit(tfidf_df)
            normalized_df = self.scaler_model.transform(tfidf_df).withColumnRenamed("scaled_features", "features")
        elif normalization == 'sigmoid':
            sigmoid_udf = udf(lambda v: v.toArray() / (1 + v.toArray()), VectorUDT())
            normalized_df = tfidf_df.withColumn("features", sigmoid_udf(col(features_col)))
        else:
            normalized_df = tfidf_df.withColumnRenamed(features_col, "features")

        return normalized_df

    def transform(self, df):
        """
        Transform new data using the fitted TF-IDF and normalization.

        Args:
            df: Spark DataFrame with columns `BlockId` and `EventSequence`.

        Returns:
            Transformed Spark DataFrame with features.
        """
        # Tokenize the EventSequence if necessary
        tokenize_udf = udf(lambda x: x, ArrayType(StringType()))  # Assuming EventSequence is already tokenized
        df = df.withColumn("tokens", tokenize_udf(col("EventSequence")))

        # Transform using HashingTF and IDF
        tf_df = self.hashingTF.transform(df)
        tfidf_df = self.idf_model.transform(tf_df) if self.idf_model else tf_df

        # Apply normalization if specified
        if self.normalization == 'zero-mean':
            normalized_df = self.scaler_model.transform(tfidf_df).withColumnRenamed("scaled_features", "features")
        elif self.normalization == 'sigmoid':
            sigmoid_udf = udf(lambda v: v.toArray() / (1 + v.toArray()), VectorUDT())
            normalized_df = tfidf_df.withColumn("features", sigmoid_udf(col("tfidf_features")))
        else:
            normalized_df = tfidf_df.withColumnRenamed("tfidf_features", "features")

        return normalized_df

def _split_data_spark(data_df, train_ratio=0.5, split_type='uniform'):
    """
    Split data into training and testing sets using PySpark.

    Arguments
    ---------
        data_df: PySpark DataFrame, should contain 'EventSequence' and optionally 'Label'.
        train_ratio: float, the ratio of training data for train/test split.
        split_type: str, 'uniform' or 'sequential'.

    Returns
    -------
        train_df: PySpark DataFrame, training data.
        test_df: PySpark DataFrame, testing data.
    """
    if split_type == 'uniform' and 'Label' in data_df.columns:
        pos_df = data_df.filter(col("Label") == 1)
        neg_df = data_df.filter(col("Label") == 0)

        pos_train_size = int(pos_df.count() * train_ratio)
        neg_train_size = int(neg_df.count() * train_ratio)

        pos_train_df = pos_df.limit(pos_train_size)
        pos_test_df = pos_df.subtract(pos_train_df)
        neg_train_df = neg_df.limit(neg_train_size)
        neg_test_df = neg_df.subtract(neg_train_df)

        train_df = pos_train_df.union(neg_train_df).orderBy(rand())
        test_df = pos_test_df.union(neg_test_df).orderBy(rand())

    elif split_type == 'sequential':
        total_count = data_df.count()
        train_count = int(total_count * train_ratio)

        train_df = data_df.limit(train_count)
        test_df = data_df.subtract(train_df)

    else:
        raise ValueError("Invalid split_type. Must be 'uniform' or 'sequential'.")

    return train_df, test_df

def load_HDFS_spark(struct_log, label_data=None, window='session', train_ratio=0.5, split_type='sequential', save_csv=False, window_size=0):
    """
    Load HDFS structured log into train and test data using PySpark.

    Arguments
    ---------
        struct_log: PySpark DataFrame, contains structured log data with at least 'Content' and 'EventId'.
        label_data: PySpark DataFrame, contains labels with columns 'BlockId' and 'Label', optional.
        window: str, the window options including `session` (default).
        train_ratio: float, the ratio of training data for train/test split.
        split_type: str, 'uniform' or 'sequential', determines how to split the dataset.
        save_csv: bool, whether to save the processed dataset as CSV.
        window_size: int, the size of the window (default is 0 for no windowing).

    Returns
    -------
        Train and test datasets in PySpark DataFrames or windowed DataFrames.
    """
    assert window == 'session', "Only window=session is supported for HDFS dataset."

    extract_blk_udf = udf(lambda content: list(set(re.findall(r'(blk_-?\d+)', content))), ArrayType(StringType()))
    struct_log = struct_log.withColumn("BlockIds", extract_blk_udf(col("Content")))

    exploded_df = struct_log.withColumn("BlockId", explode(col("BlockIds"))).select("BlockId", "EventId")
    
    data_df = exploded_df.groupBy("BlockId").agg(collect_list("EventId").alias("EventSequence"))
    
    if label_data is not None:
        label_data = label_data.withColumn("Label", (col("Label") == "Anomaly").cast("int"))
        data_df = data_df.join(label_data, on="BlockId", how="left").fillna(0, subset=["Label"])
    
    train_df, test_df = _split_data_spark(data_df, train_ratio=train_ratio, split_type=split_type)
    
    if save_csv:
        train_df.write.csv("train_data.csv", header=True, mode="overwrite")
        test_df.write.csv("test_data.csv", header=True, mode="overwrite")
    
    if window_size > 0:
        train_df = slice_hdfs_spark(train_df, window_size)
        test_df = slice_hdfs_spark(test_df, window_size)
        print("Slicing complete.")
        
    return train_df, test_df

def slice_hdfs_spark(data_df, window_size):
    """
    Slice event sequences into fixed-size windows using PySpark.

    Arguments
    ---------
        data_df: PySpark DataFrame, should contain 'EventSequence' and 'Label'.
        window_size: int, the size of the sliding window.

    Returns
    -------
        sliced_df: PySpark DataFrame, contains windowed sequences with labels.
    """
    def slice_sequence(session_id, sequence, label):
        """
        Slice a single sequence into windows.
        """
        results = []
        for i in range(len(sequence) - window_size + 1):
            window = sequence[i:i + window_size]
            next_event = sequence[i + window_size] if i + window_size < len(sequence) else "#Pad"
            results.append((session_id, window, next_event, label))
        return results

    slice_udf = udf(
        lambda session_id, sequence, label: slice_sequence(session_id, sequence, label),
        ArrayType(StructType([
            StructField("SessionId", IntegerType(), True),
            StructField("EventSequence", ArrayType(StringType()), True),
            StructField("Label", StringType(), True),
            StructField("SessionLabel", IntegerType(), True)
        ]))
    )

    sliced_df = (
        data_df.withColumn("SlicedSequences", slice_udf(col("SessionId"), col("EventSequence"), col("Label")))
               .selectExpr("explode(SlicedSequences) as Sliced")
               .select(
                   col("Sliced.SessionId"),
                   col("Sliced.EventSequence"),
                   col("Sliced.Label"),
                   col("Sliced.SessionLabel")
               )
    )

    return sliced_df