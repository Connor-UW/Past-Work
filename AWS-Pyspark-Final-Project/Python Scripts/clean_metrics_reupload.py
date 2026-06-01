# SCRIPT TO CLEAN, CREATE KEY METRICS, AND REUPLOAD
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, sum as _sum, avg, count

# Building the Spark Session
spark = SparkSession.builder     .appName("DataClean_FeatureEngineering")     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .getOrCreate()
# Creating a Spark dataframe from transactions.csv
df = spark.read.csv(
    "s3a://bigdata-connoriu-mini-project/raw/transactions.csv",
    header=True, inferSchema=True
)
# Cleaning the data
df = df.dropna(subset=["Transaction_date", "Transaction_ID", "Amount_spent"])
df = df.withColumn("Amount_spent", col("Amount_spent").cast("double"))
df = df.withColumn("Age", col("Age").cast("int"))
df = df.withColumn("Transaction_date", to_date(col("Transaction_date")))
df = df.withColumn("year", year(col("Transaction_date")))
df = df.withColumn("month", month(col("Transaction_date")))
df = df.withColumn("total_amount", col("Amount_spent"))

# Calculating key metrics
# 1. Revenue by State (ranked highest to lowest)
state_revenue = df.groupBy("State_names").agg(
    _sum("total_amount").alias("total_revenue")
).orderBy(col("total_revenue").desc())

# 2. Revenue by Year (ranked highest to lowest)
yearly_revenue = df.groupBy("year").agg(
    _sum("total_amount").alias("total_revenue")
).orderBy(col("total_revenue").desc())

# 3. Monthly avg revenue
monthly_avg = df.groupBy("month").agg(
    avg("total_amount").alias("avg_revenue")
).orderBy(col("month"))
# 4. Median Amount Spent per Purchase for each State (ranked highest to lowest)
from pyspark.sql.functions import percentile_approx
state_median_spend = df.groupBy("State_names").agg(
    percentile_approx("Amount_spent", 0.5).alias("median_spent")
).orderBy(col("median_spent").desc())


# 5. Most Common Payment Methods
payment_popularity = df.groupBy("Payment_method").agg(
    count("*").alias("usage_count")
).orderBy(col("usage_count").desc())

# Making the following for loop’s life easier
outputs = {
    "State_Revenue_Ranking": state_revenue,
    "Yearly_Revenue_Summary": yearly_revenue,
    "Monthly_Revenue_Averages": monthly_avg,
    "Median_Spend_Per_State": state_median_spend,
    "Payment_Preference": payment_popularity
}
# A print for loop just to quickly check results
for name, table in outputs.items():
    print(f"\n{name}")
    table.show(15, truncate=False)

# Uploading back to s3
root = "s3a://bigdata-connoriu-mini-project/processed"
df.write.mode("overwrite").csv(f"{root}/clean_transactions", header=True)
state_revenue.write.mode("overwrite").csv(f"{root}/state_revenue", header=True)
yearly_revenue.write.mode("overwrite").csv(f"{root}/yearly_revenue", header=True)
monthly_avg.write.mode("overwrite").csv(f"{root}/monthly_revenue_averages", header=True)
state_median_spend.write.mode("overwrite").csv(f"{root}/median_spend_per_state", header=True)
payment_popularity.write.mode("overwrite").csv(f"{root}/payment_preferences", header=True)
print(root)
spark.stop()
