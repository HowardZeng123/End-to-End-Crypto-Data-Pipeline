# End-to-End Crypto Data Pipeline
<img width="1331" height="1368" alt="diagram-export-26-03-2026-13_34_18" src="https://github.com/user-attachments/assets/e61a334d-475d-4cd5-87e8-c0f810f427a9" />

Real-time crypto data platform built with Python, Kafka, Spark, MinIO, and PostgreSQL using a medallion architecture.

## What This Project Does
- Ingests market data from Binance WebSocket via a Python producer.
- Streams events into a 3-node Kafka cluster.
- Lands raw events into MinIO Bronze using Kafka Connect S3 Sink.
- Cleans and standardizes data into MinIO Silver with PySpark.
- Produces Gold analytics (daily summaries and streaming indicators) and stores results in PostgreSQL.
- Sends near real-time alert notifications from Spark Streaming.

## Architecture
- Data source: Binance WebSocket
- Ingestion: Python producer
- Messaging: Kafka (3 brokers)
- Landing: Kafka Connect S3 Sink -> MinIO Bronze
- Processing: PySpark batch + Structured Streaming
- Serving: PostgreSQL
- Alerting: Discord webhook

Flowchart placeholder:
- You can paste your Mermaid flowchart here later.

## Buckets and Zones
- Bronze: raw JSON stream snapshots
- Silver: cleansed Parquet datasets
- Gold: curated aggregates for analytics and reporting

## Quick Start
1. Copy environment template:
   - cp .env.example .env
2. Fill all required values in .env (do not commit this file).
3. Start infrastructure:
   - docker compose up -d
4. Start producer from host:
   - python producer.py
5. Submit Spark jobs as needed:
   - silver job, batch job, streaming analytics, stream alert

## Security Notes
- No production secrets are committed.
- Credentials and webhook are loaded from environment variables.
- .env is ignored by git.

## Main Components
- producer.py
- spark_jobs/silver_job.py
- spark_jobs/batch_job.py
- spark_jobs/streaming_job.py
- spark_jobs/stream_alert_job.py
- sink.json
- docker-compose.yml
