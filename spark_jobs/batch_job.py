import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, avg, date_format

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://silver-lake/bitcoin_price")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY/MINIO_SECRET_KEY chưa được cấu hình")

if not POSTGRES_DB or not POSTGRES_USER or not POSTGRES_PASSWORD:
    raise RuntimeError("POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD chưa được cấu hình")

spark = SparkSession.builder \
        .appName("CryptoDailyBatch_MinIO") \
        .getOrCreate()

hconf = spark.sparkContext._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hconf.set("fs.s3a.connection.ssl.enabled", "false")

spark.sparkContext.setLogLevel("WARN")

print("📥 Đang kéo dữ liệu silver từ MinIO...")
df = spark.read.parquet(SILVER_PATH)

print("📊 Đang tổng hợp dữ liệu theo ngày...")
daily_summary_df = df \
    .withColumn("date", date_format(col("ingest_time"), "yyyy-MM-dd")) \
    .groupBy("date", "symbol") \
    .agg(
        max("price_usd").alias("high_price"),
        min("price_usd").alias("low_price"),
        avg("price_usd").alias("avg_price")
    )

jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

daily_summary_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "bitcoin_daily_summary") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

print("✅ BATCH JOB HOÀN THÀNH!")
