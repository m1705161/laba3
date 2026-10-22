"""Streaming-загрузка: Kafka (топик weather) → Iceberg (append-only).

Никакого SCD/CDC — просто читаем поток и дописываем строки в таблицу.
Каждый микро-батч = новый снапшот Iceberg, поэтому Time Travel работает.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_TOPIC = "weather"
KAFKA_BOOTSTRAP = "host.docker.internal:9092"
TARGET_TABLE = "scd.raw.weather"

# Схема JSON-сообщения продюсера (все поля — из producer.py).
SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("observed_at", StringType(), True),
    StructField("fetched_at", StringType(), True),
])

spark = (SparkSession.builder
         .appName("stream-ingest-weather")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Целевая таблица (append-only — MERGE и format-version=2 не нужны).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        city STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        temperature_2m DOUBLE,
        relative_humidity_2m DOUBLE,
        wind_speed_10m DOUBLE,
        observed_at STRING,
        fetched_at STRING
    ) USING iceberg
""")
print(f"[ingest] target table {TARGET_TABLE} ready")

# 2. Поток из Kafka.
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

# 3. Парсим JSON: каждая строка = одно наблюдение о погоде.
events = (raw
          .selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), SCHEMA).alias("d"))
          .select("d.*"))


def append_batch(batch_df, epoch_id):
    """Просто дописываем микро-батч в Iceberg (append)."""
    batch_df.writeTo(TARGET_TABLE).append()
    print(f"[ingest] batch #{epoch_id} processed")


query = (events.writeStream
         .foreachBatch(append_batch)
         .outputMode("append")
         .option("checkpointLocation", "/opt/spark/checkpoint")
         .trigger(processingTime="10 seconds")
         .start())

print("[ingest] streaming started, waiting for weather events...")
query.awaitTermination()
