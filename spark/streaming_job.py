import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Load environment variables
load_dotenv()

# Structured Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("SparkStreamingJob")

# Configuration
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "/tmp/delta/bronze")
SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "/tmp/delta/silver")
GOLD_PATH = os.getenv("DELTA_GOLD_PATH", "/tmp/delta/gold")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_LOCATION", "/tmp/delta/_checkpoints")

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True)
])

def create_spark_session():
    builder = SparkSession.builder \
        .appName("LakehouseStreamingPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")

    return configure_spark_with_delta_pip(builder, extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "io.delta:delta-core_2.12:2.4.0"
    ]).getOrCreate()

def upsert_to_delta(micro_batch_df, batch_id, target_path):
    logger.info(f"Processing micro-batch {batch_id} for path {target_path}")
    spark = micro_batch_df.sparkSession
    deduped_df = micro_batch_df.dropDuplicates(["event_id"])
    
    if not DeltaTable.isDeltaTable(spark, target_path):
        logger.info(f"Initializing Delta table at {target_path}")
        deduped_df.write.format("delta").mode("overwrite").save(target_path)
    else:
        target_table = DeltaTable.forPath(spark, target_path)
        target_table.alias("t").merge(
            deduped_df.alias("s"),
            "t.event_id = s.event_id"
        ).whenNotMatchedInsertAll().execute()

def run_pipeline():
    spark = create_spark_session()
    logger.info("Spark session initialized. Starting stream ingestion...")
    
    # 1. READ FROM KAFKA
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", "user_events") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", 10000) \
        .load()

    parsed_df = raw_stream.select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("event_time", expr("cast(timestamp/1000 as timestamp)"))

    # 2. SILVER LAYER (Idempotent Merge)
    silver_query = parsed_df.writeStream \
        .foreachBatch(lambda df, b_id: upsert_to_delta(df, b_id, SILVER_PATH)) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver") \
        .trigger(processingTime='10 seconds') \
        .start()

    # 3. GOLD LAYER (Incremental Aggregation)
    gold_df = parsed_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(window(col("event_time"), "1 minute"), col("event_type")).count()

    gold_query = gold_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold") \
        .option("mergeSchema", "true") \
        .start(GOLD_PATH)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_pipeline()
