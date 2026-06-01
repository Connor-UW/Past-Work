# SCRIPT TO JUST LOAD DATASET
from pyspark.sql import SparkSession

# Building the Spark Session
spark = SparkSession.builder     .appName("S3LoadTest")     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .getOrCreate()

# Creating a Spark dataframe from transactions.csv
df = spark.read.csv(
    "s3a://bigdata-connoriu-mini-project/raw/transactions.csv",
    header=True,
    inferSchema=True
)

# Quickly viewing the dataset to make sure it worked
print("\n==============================")
print("Row Count:", df.count())
print("Showing first 10 rows:\n")
df.show(10, truncate=False)

spark.stop()
