import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bitcoin_price")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not POSTGRES_DB or not POSTGRES_USER or not POSTGRES_PASSWORD:
    raise RuntimeError("POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD chưa được cấu hình")

spark = SparkSession.builder \
    .appName("CryptoRealTimeProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ingest_time", StringType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = parsed_df.withColumn("timestamp", col("timestamp").cast("double").cast("timestamp"))

windowed_counts = processed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minutes"),
        col("symbol")
    ) \
    .agg(avg("price_usd").alias("avg_price")) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("avg_price")
    )


def write_to_postgres(batch_df, batch_id):
    jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bitcoin_analytics") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    print(f"Batch {batch_id} processed.")


query = windowed_counts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .start()

print("Streaming Job is running...")
query.awaitTermination()
