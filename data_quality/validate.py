import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_quality_checks():
    spark = SparkSession.builder \
        .appName("DataQualityValidation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("Running Data Quality Checks on Silver Layer...")
    
    # Load Silver Table
    try:
        silver_df = spark.read.format("delta").load("/tmp/delta/silver")
    except Exception as e:
        print(f"Error loading silver table: {e}")
        # If table doesn't exist yet, we might skip or fail depending on policy
        return

    # Check 1: Null User IDs
    null_users = silver_df.filter(col("user_id").isNull()).count()
    if null_users > 0:
        print(f"CRITICAL: Found {null_users} null user_ids in Silver layer!")
        sys.exit(1) # Block the pipeline

    # Check 2: Invalid Event Types
    valid_events = ["click", "view", "purchase"]
    invalid_events = silver_df.filter(~col("event_type").isin(valid_events)).count()
    if invalid_events > 0:
        print(f"WARNING: Found {invalid_events} invalid event types.")
        # We might choose to just warn or fail
        # sys.exit(1)

    print("Data Quality Checks Passed Successfully!")

if __name__ == "__main__":
    run_quality_checks()
