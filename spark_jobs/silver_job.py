import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://bitcoin-lake/topics/bitcoin_price/*/*")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://silver-lake/bitcoin_price/")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY/MINIO_SECRET_KEY chưa được cấu hình")

spark = SparkSession.builder \
    .appName("Crypto_Bronze_to_Silver") \
    .getOrCreate()

hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark.sparkContext.setLogLevel("WARN")

raw_df = spark.read.json(BRONZE_PATH)

print("cleansing data...")
clean_df = raw_df \
    .dropna(subset=["price_usd", "timestamp", "symbol"]) \
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp")))) \
    .filter(col("price_usd") > 0)

print("Đang sang tầng Silver định dạng PARQUET...")
clean_df.write \
    .mode("overwrite") \
    .parquet(SILVER_PATH)
