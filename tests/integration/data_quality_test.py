from pyspark.sql import SparkSession
from tests.config import DELTA_SILVER_PATH

def validate_data():
    print("[DQ] Running data quality checks...")
    spark = SparkSession.builder.getOrCreate()
    
    try:
        df = spark.read.format("delta").load(DELTA_SILVER_PATH)
        
        # 1. Null Checks
        null_count = df.filter("user_id IS NULL").count()
        if null_count > 0:
            print(f"[DQ] FAIL: {null_count} null user_id found!")
            return False
            
        # 2. Schema Validation (Event Types)
        valid_types = ["click", "view", "purchase"]
        invalid = df.filter(~df.event_type.isin(valid_types)).count()
        if invalid > 0:
            print(f"[DQ] FAIL: {invalid} invalid event types found!")
            return False
            
        # 3. Freshness Check (Optional)
        # current_time = time.time() * 1000
        # ... logic to check if data is recent ...

        print("[DQ] PASSED")
        return True
    except Exception as e:
        print(f"[DQ] ERROR during validation: {e}")
        return False

if __name__ == "__main__":
    validate_data()
