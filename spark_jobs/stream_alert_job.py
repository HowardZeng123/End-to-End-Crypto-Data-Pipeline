import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bitcoin_price")


def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ DISCORD_WEBHOOK_URL chưa được cấu hình, bỏ qua gửi alert.")
        return

    try:
        payload = {"content": f"🚨 **CRYPTO ALERT:** {message}"}
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code >= 400:
            print(f"❌ Discord trả về lỗi HTTP {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ Lỗi gửi Discord: {e}")


spark = SparkSession.builder \
        .appName("CryptoAlert_Streaming") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("symbol", StringType()),
    StructField("price_usd", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("ingest_time", StringType())
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

last_reported_prices = {}


def process_batch(batch_df, batch_id):
    global last_reported_prices

    base_df = batch_df.alias("src")
    latest_ts_df = batch_df.groupBy("symbol").agg(spark_max("timestamp").alias("max_ts")).alias("latest")

    latest_rows_df = base_df.join(
        latest_ts_df,
        (col("src.symbol") == col("latest.symbol")) & (col("src.timestamp") == col("latest.max_ts")),
        "inner"
    ).select(
        col("src.symbol").alias("symbol"),
        col("src.price_usd").alias("price_usd"),
        col("src.timestamp").alias("timestamp")
    ).dropDuplicates(["symbol"])

    rows = latest_rows_df.collect()
    if not rows:
        return

    for row in rows:
        symbol = row["symbol"]
        current_price = row["price_usd"]
        previous_price = last_reported_prices.get(symbol)

        if previous_price is None:
            msg = f"🟢 **BẮT ĐẦU THEO DÕI {symbol}:** Giá hiện tại là ${current_price:,.2f}"
        else:
            diff = current_price - previous_price
            pct_change = 0.0 if previous_price == 0 else (diff / previous_price) * 100

            if diff > 0:
                icon = "🚀"
                trend = f"TĂNG +${diff:,.2f} (+{pct_change:.2f}%)"
            elif diff < 0:
                icon = "🩸"
                trend = f"GIẢM -${abs(diff):,.2f} ({pct_change:.2f}%)"
            else:
                icon = "⏸️"
                trend = "ĐI NGANG (0.00%)"

            msg = (
                f"{icon} **{symbol}:** ${current_price:,.2f} | {trend} "
                f"| (Giá cũ: ${previous_price:,.2f})"
            )

        print(f"✅ {msg}")
        send_discord_alert(msg)
        last_reported_prices[symbol] = current_price


print("📺 Đang lắng nghe data real-time...")
parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="30 seconds") \
    .start() \
    .awaitTermination()
