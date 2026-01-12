from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = SparkSession.builder \
    .appName("WeatherAlertsStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma Kafka message
schema = StructType([
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("weathercode", DoubleType())
])

# Lecture Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_stream") \
    .load()

# Conversion JSON
df = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Timestamp
df = df.withColumn(
    "event_time",
    to_timestamp("event_time")
)

# Alertes vent
df = df.withColumn(
    "wind_alert_level",
    when(col("windspeed") < 10, "level_0")
    .when((col("windspeed") >= 10) & (col("windspeed") <= 20), "level_1")
    .otherwise("level_2")
)

# Alertes chaleur
df = df.withColumn(
    "heat_alert_level",
    when(col("temperature") < 25, "level_0")
    .when((col("temperature") >= 25) & (col("temperature") <= 35), "level_1")
    .otherwise("level_2")
)

# Sélection finale
df_out = df.select(
    "event_time",
    "latitude",
    "longitude",
    "temperature",
    "windspeed",
    "wind_alert_level",
    "heat_alert_level"
)

# Sortie Kafka
query = df_out.selectExpr(
    "to_json(struct(*)) AS value"
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/spark-weather-checkpoint") \
    .start()

query.awaitTermination()
