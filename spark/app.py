from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------
# 1. Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("Airspace Intelligence") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------------------
# 2. Schema
# -----------------------------------
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("country", StringType()),
    StructField("timestamp", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("altitude", DoubleType()),
    StructField("velocity", DoubleType())
])

# -----------------------------------
# 3. Read Kafka
# -----------------------------------
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight_stream") \
    .option("startingOffsets", "latest") \
    .load()

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------------
# 🔥 DEBUG STREAM (REMOVE AFTER TEST)
# -----------------------------------
debug_query = df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# -----------------------------------
# 4. Clean (LESS STRICT)
# -----------------------------------
df_clean = df.filter(
    col("latitude").isNotNull() &
    col("longitude").isNotNull()
)

# -----------------------------------
# 5. Feature Engineering
# -----------------------------------
df_feat = df_clean \
    .withColumn("speed_kmh", col("velocity") * 3.6) \
    .withColumn("altitude_band",
        when(col("altitude") < 2000, "low")
        .when(col("altitude") < 10000, "medium")
        .otherwise("high")
    )

# -----------------------------------
# 6. Load Airports CSV (STATIC)
# -----------------------------------
df_airports = spark.read \
    .option("header", True) \
    .csv("/opt/spark-apps/airports.csv")

df_airports = df_airports.select(
    col("latitude_deg").cast("double").alias("ap_lat"),
    col("longitude_deg").cast("double").alias("ap_lon"),
    col("iso_country"),
    col("iso_region"),
    col("type")
)

# -----------------------------------
# 7. Geo Join (OPTIMIZED)
# -----------------------------------
df_joined = df_feat.join(
    broadcast(df_airports),
    (abs(df_feat.latitude - df_airports.ap_lat) < 0.5) &
    (abs(df_feat.longitude - df_airports.ap_lon) < 0.5),
    "left"
)

# -----------------------------------
# 8. Capacity Assignment
# -----------------------------------
df_joined = df_joined.withColumn(
    "base_capacity",
    when(col("type") == "large_airport", 100)
    .when(col("type") == "medium_airport", 70)
    .when(col("type") == "small_airport", 40)
    .when(col("type") == "heliport", 10)
    .otherwise(20)
)

# -----------------------------------
# 9. Weather Factor (SIMULATED for now)
# -----------------------------------
df_joined = df_joined.withColumn(
    "weather_factor",
    when(col("speed_kmh") > 800, 0.7).otherwise(1.0)
)

# -----------------------------------
# 10. Effective Capacity
# -----------------------------------
df_joined = df_joined.withColumn(
    "effective_capacity",
    col("base_capacity") * col("weather_factor")
)

# -----------------------------------
# 11. Streaming Aggregation
# -----------------------------------
aci_df = df_joined.groupBy("iso_region") \
    .agg(
        count("*").alias("aircraft_count"),
        sum("effective_capacity").alias("capacity")
    ) \
    .withColumn(
        "ACI",
        when(col("capacity") > 0,
             col("aircraft_count") / col("capacity"))
        .otherwise(0)
    )

# -----------------------------------
# 12. Output
# -----------------------------------
query = aci_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()