# SCRIPT TO GET THE DATA IN A SINGLE PARQUET FILE FOR SAGEMAKER
from pyspark.sql import SparkSession

# Building the Spark Session
spark = SparkSession.builder     .appName("RebuildSingleParquet")     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .getOrCreate()
# Loading the data set from clean_transactions
df = spark.read.csv("s3a://bigdata-connoriu-mini-project/processed/clean_transactions/", header=True, inferSchema=True)

# Forces the output to be in 1 file
df = df.coalesce(1)
df.write.mode("overwrite").parquet("s3a://bigdata-connoriu-mini-project/processed/single_clean_parquet", compression="snappy")

print("\nSuccess yipee")
spark.stop()
