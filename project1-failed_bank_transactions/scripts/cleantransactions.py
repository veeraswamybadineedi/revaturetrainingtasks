from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("FailedTransactionFilter").getOrCreate()

# Set the input and output paths in the GCS bucket
BUCKET_NAME = "bank-pro"
CLEANED_FOLDER = f"gs://{BUCKET_NAME}/cleaned/"
FAILED_FOLDER = f"gs://{BUCKET_NAME}/failed_transactions/"

# Read all cleaned CSV files from the cleaned folder
df = spark.read.option("header", "true").option("inferSchema", "true").csv(CLEANED_FOLDER)

# Filter only the failed transactions (Status is 'Failed' or 'Declined')
failed_df = df.filter(col("Transaction_Status").isin(["Failed", "Declined"]))

# Write the failed transactions to the failed folder
failed_df.write.mode("overwrite").option("header", "true").csv(FAILED_FOLDER)

print("Failed transaction filtering completed.")

# Stop the Spark session
spark.stop()
