# SCRIPT TO DO SQL PROMPTS ON CLEANED DATA
from pyspark.sql import SparkSession

# Building the Spark Session
spark = SparkSession.builder     .appName("SparkSQL_Analysis")     .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")     .config("spark.hadoop.fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")     .getOrCreate()

# Load processed data: clean transactions
df = spark.read.csv("s3a://bigdata-connoriu-mini-project/processed/clean_transactions/", header=True, inferSchema=True)
df.createOrReplaceTempView("transactions")

# 1. Checking our work with top revenue states
print("Top Revenue States")
spark.sql("""
SELECT State_names, SUM(Amount_spent) AS revenue
FROM transactions
GROUP BY State_names
ORDER BY revenue DESC
LIMIT 15
""").show()

# 2. Getting more detail in monthly revenue
print("Month-Over-Month Revenue")
spark.sql("""
SELECT year, month, SUM(Amount_spent) AS monthly_revenue
FROM transactions
GROUP BY year, month
ORDER BY year, month
""").show(24)


# 3. Ranking age brackets by occurrence
print("\n Most Common Age Brackets")
spark.sql("""
SELECT
    COALESCE(
        CONCAT(FLOOR(Age/10)*10, '-', FLOOR(Age/10)*10 + 9),
        'Unknown'
    ) AS age_group,
    COUNT(*) AS user_count
FROM transactions
GROUP BY age_group
ORDER BY user_count DESC
""").show(20, truncate=False)

# 4. How revenue changes relative to employment status
print("\n Revenue by Employment Status")
spark.sql("""
SELECT Employees_status,
       COUNT(*) AS total_transactions,
       SUM(Amount_spent) AS total_revenue,
       AVG(Amount_spent) AS avg_purchase_value,
       percentile_approx(Amount_spent, 0.5) AS median_purchase_value
FROM transactions
GROUP BY Employees_status
ORDER BY total_revenue DESC
""").show(truncate=False)

# 5. Checking the median transaction amount of the most revenue generating state
print("\n Median Transaction Amount in Illinois")
spark.sql("""
SELECT percentile_approx(Amount_spent, 0.5) AS median_spend_IL
FROM transactions
WHERE State_names = 'Illinois'
""").show()

spark.stop()
