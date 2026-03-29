from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -----------------------------------
# 1. Spark Session
# -----------------------------------
spark = SparkSession.builder \
    .appName("Airspace Intelligence") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

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
    StructField("velocity", DoubleType()),
    StructField("vertical_rate", DoubleType())
])

# -----------------------------------
# 3. Read Kafka Stream
# -----------------------------------
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight_stream") \
    .option("startingOffsets", "earliest") \
    .load()

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------------------
# 4. Data Cleaning + Validation
# -----------------------------------
df_clean = df.filter(
    (col("latitude").isNotNull()) &
    (col("longitude").isNotNull()) &
    (col("latitude").between(-90, 90)) &
    (col("longitude").between(-180, 180)) &
    (col("velocity") > 0) &
    (col("altitude") > 0)
)

# -----------------------------------
# 5. Feature Engineering
# -----------------------------------
df_feat = df_clean \
    .withColumn("speed_kmh", col("velocity") * 3.6) \
    .withColumn(
        "altitude_band",
        when(col("altitude") < 2000, "LOW")
        .when(col("altitude") < 10000, "MEDIUM")
        .otherwise("HIGH")
    ) \
    .withColumn(
        "flight_status",
        when(col("vertical_rate") > 5, "CLIMB")
        .when(col("vertical_rate") < -5, "DESCENT")
        .otherwise("STABLE")
    ) \
    .withColumn(
        "geo_region",
        concat(
            floor(col("latitude") / 5) * 5,
            lit("_"),
            floor(col("longitude") / 5) * 5
        )
    )

# -----------------------------------
# 6. Load Airports Dataset
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
# 7. Geo Join
# -----------------------------------
df_joined = df_feat.join(
    broadcast(df_airports),
    (abs(df_feat.latitude - df_airports.ap_lat) < 1.0) &
    (abs(df_feat.longitude - df_airports.ap_lon) < 1.0),
    "left"
)

# -----------------------------------
# 8. Capacity Assignment
# -----------------------------------
df_joined = df_joined.withColumn(
    "base_capacity",
    when(col("type") == "large_airport", 120)
    .when(col("type") == "medium_airport", 80)
    .when(col("type") == "small_airport", 50)
    .otherwise(30)
)

# -----------------------------------
# 9. Weather Factor
# -----------------------------------
df_joined = df_joined.withColumn(
    "weather_factor",
    when(col("speed_kmh") > 850, 0.7)
    .when(col("speed_kmh") < 200, 0.9)
    .otherwise(1.0)
)

# -----------------------------------
# 10. Effective Capacity
# -----------------------------------
df_joined = df_joined.withColumn(
    "effective_capacity",
    col("base_capacity") * col("weather_factor")
)

# -----------------------------------
# 11. Timestamp
# -----------------------------------
df_final = df_joined.withColumn(
    "event_time",
    from_unixtime(col("timestamp")).cast("timestamp")
)

# -----------------------------------
# 12. Region
# -----------------------------------
df_final = df_final.withColumn(
    "region",
    coalesce(col("iso_region"), col("geo_region"))
)

# -----------------------------------
# 13. ACI Aggregation
# -----------------------------------
aci_stream = df_final \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("region")
    ) \
    .agg(
        count("*").alias("aircraft_count"),
        avg("effective_capacity").alias("capacity")
    ) \
    .withColumn(
        "aci",
        when(col("capacity") > 0,
             col("aircraft_count") / col("capacity"))
        .otherwise(0)
    ) \
    .select(
        col("region"),
        col("window.start").alias("timestamp"),
        col("aircraft_count"),
        col("capacity"),
        col("aci")
    )

# -----------------------------------
# 14. RDD Processing (BEST VERSION)
# -----------------------------------
def process_batch(batch_df, batch_id):

    print(f"\n===== Batch {batch_id} =====")

    # -----------------------------------
    # 1. RDD: Aircraft Count per Region
    # -----------------------------------
    region_counts_rdd = (
        batch_df.select("region").rdd
        .map(lambda x: (x["region"], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    print("Top Regions:", region_counts_rdd.takeOrdered(5, key=lambda x: -x[1]))

    # -----------------------------------
    # 2. RDD: Avg Speed per Region
    # -----------------------------------
    avg_speed_rdd = (
        batch_df.select("region", "speed_kmh").rdd
        .map(lambda x: (x["region"], (x["speed_kmh"], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: x[0] / x[1])
    )

    print("Avg Speed:", avg_speed_rdd.take(5))

    # -----------------------------------
    # 3. SQL Analytics
    # -----------------------------------
    batch_df.createOrReplaceTempView("flights")

    flights_country = spark.sql("""
        SELECT country, COUNT(*) AS total_flights
        FROM flights
        GROUP BY country
        ORDER BY total_flights DESC
    """)

    altitude_country = spark.sql("""
        SELECT country, AVG(altitude) AS avg_altitude
        FROM flights
        GROUP BY country
    """)

    # -----------------------------------
    # 4. COUNTRY METRICS
    # -----------------------------------
    country_metrics = flights_country.join(
        altitude_country, "country"
    ).withColumn(
        "traffic_density",
        col("total_flights") / 100.0
    ).withColumn(
        "timestamp",
        current_timestamp()
    )

    # -----------------------------------
    # 5. REGION METRICS (ACI)
    # -----------------------------------
    region_metrics = batch_df.groupBy("region").agg(
        count("*").alias("aircraft_count"),
        avg("effective_capacity").alias("capacity")
    ).withColumn(
        "aci",
        col("aircraft_count") / col("capacity")
    ).withColumn(
        "timestamp",
        current_timestamp()
    )

    # -----------------------------------
    # 6. WRITE TO CASSANDRA
    # -----------------------------------
    region_metrics.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="region_metrics", keyspace="airspace") \
        .save()

    country_metrics.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="country_metrics", keyspace="airspace") \
        .save()

# -----------------------------------
# 15. Streaming Write
# -----------------------------------
query = df_final.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()