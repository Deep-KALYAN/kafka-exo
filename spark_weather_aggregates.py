from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    avg, min, max, sum, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# spark = SparkSession.builder \
#     .appName("WeatherAggregatesStreaming") \
#     .getOrCreate()
spark = SparkSession.builder \
    .appName("WeatherAggregatesStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma du topic weather_transformed
schema = StructType([
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType())
])

# Lecture Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

# Parse JSON
df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Timestamp
df = df.withColumn(
    "event_time",
    to_timestamp("event_time")
)

# Flags d’alertes (level_1 ou level_2)
df = df.withColumn(
    "wind_alert_flag",
    when(col("wind_alert_level").isin("level_1", "level_2"), 1).otherwise(0)
).withColumn(
    "heat_alert_flag",
    when(col("heat_alert_level").isin("level_1", "level_2"), 1).otherwise(0)
)

# Fenêtre glissante
agg_df = df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    avg("temperature").alias("temp_avg"),
    min("temperature").alias("temp_min"),
    max("temperature").alias("temp_max"),
    sum("wind_alert_flag").alias("wind_alerts_count"),
    sum("heat_alert_flag").alias("heat_alerts_count")
)

# Sortie console (debug pédagogique)
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
