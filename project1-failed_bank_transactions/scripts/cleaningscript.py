from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, trim
from pyspark.sql.types import DoubleType, StringType

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionDataCleaning").getOrCreate()

# Set the input and output paths in the GCS bucket
BUCKET_NAME = "bank-pro"
RAW_FOLDER = "gs://" + BUCKET_NAME + "/raw_data/**"
CLEANED_FOLDER = "gs://" + BUCKET_NAME + "/cleaned/"

# Read all CSV files from the raw folder
df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_FOLDER)

# Data Cleaning Steps
# 1. Remove duplicate rows
df = df.dropDuplicates()

# 2. Convert Transaction_Date to proper date format
df = df.withColumn("Transaction_Date", to_date(col("Transaction_Date"), "yyyy-MM-dd"))

# 3. Remove rows with invalid or missing dates
df = df.na.drop(subset=["Transaction_Date"])

# 4. Convert numeric columns to proper types
df = df.withColumn("Transaction_Amount", col("Transaction_Amount").cast(DoubleType()))
df = df.withColumn("Fee_Charged", col("Fee_Charged").cast(DoubleType()))

df = df.na.fill({"Transaction_Amount": 0, "Fee_Charged": 0})

# 5. Clean branch codes by removing spaces and trimming text
df = df.withColumn("Branch_Code", trim(regexp_replace(col("Branch_Code"), "\s+", "")))

df.show(5)

# Write the cleaned data back to the cleaned folder
df.write.mode("overwrite").option("header", "true").csv(CLEANED_FOLDER)

print("Data cleaning completed.")

# Stop the Spark session
spark.stop()
