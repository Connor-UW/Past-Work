# SCRIPT TO CHECK FOR OUTLIERS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, mean, stddev

# Building the Spark Session
spark = SparkSession.builder     .appName("StdDevOutlierScan")     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .getOrCreate()

# Creating a Spark dataframe from transactions.csv
df = spark.read.csv(
    "s3a://bigdata-connoriu-mini-project/raw/transactions.csv",
    header=True, inferSchema=True
)

# Casting specific columns to other types for analysis
df = df.withColumn("Amount_spent", col("Amount_spent").cast("double"))        .withColumn("Age", col("Age").cast("int"))

# The target columns to check for outliers
columns_to_test = ["Amount_spent", "Age"]

# Calculating the relevant metrics
summary = []
for c in columns_to_test:
    stats = df.select(
        mean(col(c)).alias("mean"),
        stddev(col(c)).alias("stddev"),
        min(col(c)).alias("min"),
        max(col(c)).alias("max")
    ).toPandas().iloc[0]
    summary.append([
        c,
        float(stats["min"]),
        float(stats["max"]),
        float(stats["mean"]),
        float(stats["stddev"]),
        float(stats["mean"] - 3*stats["stddev"]),
        float(stats["mean"] + 3*stats["stddev"])
    ])


# Creating a summary table to make the print easier to read
summary_table = spark.createDataFrame(summary, ["Column", "Min", "Max", "Mean", "StdDev", "3 Cutoff (Lower)", "3 Cutoff (Upper)"])

print("\n Outlier Report: 3 standard deviations \n")
summary_table.show(truncate=False)

spark.stop()
