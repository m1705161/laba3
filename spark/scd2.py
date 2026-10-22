"""
SCD Type 2 на CDC-потоке Debezium.

Читает Kafka-топик cdc.public.customers (Debezium-события из PostgreSQL),
применяет SCD Type 2 и пишет результат в Iceberg-таблицу scd.dim.customers.

Логика SCD2 в foreachBatch (на каждом микро-батче):
  1) для UPDATE и DELETE закрываем текущую версию (valid_to = время события, is_current = false);
  2) для INSERT / UPDATE / snapshot вставляем новую версию (valid_from = время события).
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

KAFKA_TOPIC = "cdc.public.customers"
KAFKA_BOOTSTRAP = "host.docker.internal:9092"
TARGET_TABLE = "scd.dim.customers"

# Схема одной строки "after"/"before" (берём только id/name/city — остальное не нужно).
ROW_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
])

# Схема Debezium-конверта (value.converter.schemas.enable=false -> без обёртки "schema/payload").
PAYLOAD_SCHEMA = StructType([
    StructField("before", ROW_SCHEMA, True),
    StructField("after", ROW_SCHEMA, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
])

spark = (SparkSession.builder
         .appName("scd2-cdc-customers")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Целевая SCD2-таблица (format-version 2 обязателен для MERGE/row-level операций Iceberg).
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        id          BIGINT,
        name        STRING,
        city        STRING,
        valid_from  TIMESTAMP,
        valid_to    TIMESTAMP,
        is_current  BOOLEAN
    ) USING iceberg
    TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='snappy')
""")
print(f"[scd2] target table {TARGET_TABLE} ready")

# 2. CDC-поток из Kafka.
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", KAFKA_TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

# 3. Парсим Debezium JSON: строка берётся из after (или before, если это DELETE),
#    время изменения — из ts_ms конверта.
events = (raw
          .selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), PAYLOAD_SCHEMA).alias("e"))
          .select(
              coalesce(col("e.after.id"), col("e.before.id")).alias("id"),
              coalesce(col("e.after.name"), col("e.before.name")).alias("name"),
              coalesce(col("e.after.city"), col("e.before.city")).alias("city"),
              col("e.op").alias("op"),
              to_timestamp(col("e.ts_ms") / 1000).alias("ts"),
          ))


def apply_scd2(batch_df, epoch_id):
    """Один микро-батч: закрываем старые версии, вставляем новые."""
    rows = batch_df.collect()
    if not rows:
        return

    # Материализуем батч в обычный DataFrame: temp view над streaming-планом
    # не резолвится в batch-запросе MERGE (для учебной лабы объём крошечный).
    local = spark.createDataFrame(rows, schema=batch_df.schema)
    local.createOrReplaceTempView("updates")

    # (a) Закрыть текущие версии для UPDATE и DELETE.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW close_rows AS
        SELECT id, ts FROM updates WHERE op IN ('u', 'd')
    """)
    spark.sql("""
        MERGE INTO scd.dim.customers AS t
        USING close_rows AS s
        ON t.id = s.id AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.ts, t.is_current = false
    """)

    # (b) Вставить новую версию для INSERT, UPDATE и snapshot-чтения.
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW new_rows AS
        SELECT id, name, city, ts FROM updates WHERE op IN ('c', 'r', 'u')
    """)
    spark.sql("""
        MERGE INTO scd.dim.customers AS t
        USING new_rows AS s
        ON t.id = s.id AND t.is_current = true
        WHEN NOT MATCHED THEN INSERT (id, name, city, valid_from, valid_to, is_current)
        VALUES (s.id, s.name, s.city, s.ts, NULL, true)
    """)

    print(f"[scd2] batch #{epoch_id} processed")


query = (events.writeStream
         .foreachBatch(apply_scd2)
         .outputMode("append")
         .option("checkpointLocation", "/opt/spark/checkpoint")
         .trigger(processingTime="3 seconds")
         .start())

print("[scd2] streaming started, waiting for CDC events...")
query.awaitTermination()
