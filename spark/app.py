from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from ml.anomaly      import AnomalyDetector
from ml.clustering   import FlightClustering
from ml.forecasting  import TrafficForecaster
from ml.optimization import OptimizationEngine
from ml.alerts       import AlertEngine

anomaly_model  = AnomalyDetector()
cluster_model  = FlightClustering()
forecaster     = TrafficForecaster()
optimizer      = OptimizationEngine()
alert_engine   = AlertEngine()

spark = SparkSession.builder \
    .appName("Airspace Intelligence") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

schema = StructType([
    StructField("icao24",        StringType()),
    StructField("callsign",      StringType()),
    StructField("country",       StringType()),
    StructField("timestamp",     LongType()),
    StructField("longitude",     DoubleType()),
    StructField("latitude",      DoubleType()),
    StructField("altitude",      DoubleType()),
    StructField("velocity",      DoubleType()),
    StructField("vertical_rate", DoubleType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "flight_stream") \
    .option("startingOffsets", "latest") \
    .load()

df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")

df_clean = df.filter(
    F.col("latitude").isNotNull() &
    F.col("longitude").isNotNull() &
    F.col("latitude").between(-90, 90) &
    F.col("longitude").between(-180, 180) &
    (F.col("velocity") > 0) &
    (F.col("altitude") > 0)
)

df_feat = df_clean \
    .withColumn("speed_kmh", F.col("velocity") * 3.6) \
    .withColumn("altitude_band",
        F.when(F.col("altitude") < 2000, "LOW")
        .when(F.col("altitude") < 10000, "MEDIUM")
        .otherwise("HIGH")
    ) \
    .withColumn("flight_status",
        F.when(F.col("vertical_rate") > 5,  "CLIMB")
        .when(F.col("vertical_rate") < -5, "DESCENT")
        .otherwise("STABLE")
    ) \
    .withColumn("geo_region",
        F.concat(
            (F.floor(F.col("latitude")  / 5) * 5).cast("string"),
            F.lit("_"),
            (F.floor(F.col("longitude") / 5) * 5).cast("string")
        )
    )

df_airports = spark.read.option("header", True).csv("/opt/spark-apps/airports.csv")
df_airports = df_airports.select(
    F.col("latitude_deg").cast("double").alias("ap_lat"),
    F.col("longitude_deg").cast("double").alias("ap_lon"),
    F.col("iso_country"),
    F.col("iso_region"),
    F.col("type")
)

df_joined = df_feat.join(
    F.broadcast(df_airports),
    (F.abs(df_feat.latitude  - df_airports.ap_lat) < 1.0) &
    (F.abs(df_feat.longitude - df_airports.ap_lon) < 1.0),
    "left"
)

df_joined = df_joined \
    .withColumn("base_capacity",
        F.when(F.col("type") == "large_airport",  120)
        .when(F.col("type") == "medium_airport", 80)
        .when(F.col("type") == "small_airport",  50)
        .otherwise(30)
    ) \
    .withColumn("weather_factor",
        F.when(F.col("speed_kmh") > 850, 0.7)
        .when(F.col("speed_kmh") < 200, 0.9)
        .otherwise(1.0)
    ) \
    .withColumn("effective_capacity", F.col("base_capacity") * F.col("weather_factor"))

df_final = df_joined \
    .withColumn("event_time", F.from_unixtime(F.col("timestamp")).cast("timestamp")) \
    .withColumn("region", F.coalesce(F.col("iso_region"), F.col("geo_region")))


def process_batch(batch_df, batch_id):
    count_val = batch_df.count()
    print(f"\n===== Batch {batch_id} | {count_val} records =====")
    if count_val == 0:
        print("  Empty batch, skipping.")
        return

    # RDD Processing
    region_counts_rdd = (
        batch_df.select("region").rdd
        .map(lambda x: (x["region"], 1))
        .reduceByKey(lambda a, b: a + b)
    )
    print("  Top Regions (RDD):", region_counts_rdd.takeOrdered(5, key=lambda x: -x[1]))

    avg_speed_rdd = (
        batch_df.select("region", "speed_kmh").rdd
        .map(lambda x: (x["region"], (x["speed_kmh"], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: int((x[0] / x[1]) * 100) / 100)
    )
    print("  Avg Speed (RDD):", avg_speed_rdd.take(5))

    region_count_dict = dict(region_counts_rdd.collect())

    # Spark SQL
    batch_df.createOrReplaceGlobalTempView("flights")

    flights_country = spark.sql("""
        SELECT country, COUNT(*) AS total_flights
        FROM global_temp.flights
        GROUP BY country ORDER BY total_flights DESC
    """)
    altitude_country = spark.sql("""
        SELECT country, AVG(altitude) AS avg_altitude
        FROM global_temp.flights GROUP BY country
    """)
    country_metrics = flights_country.join(altitude_country, "country") \
        .withColumn("traffic_density", F.col("total_flights") / 100.0) \
        .withColumn("timestamp", F.current_timestamp())

    region_metrics = batch_df.groupBy("region").agg(
        F.count("*").alias("aircraft_count"),
        F.avg("effective_capacity").alias("capacity")
    ).withColumn("aci", F.col("aircraft_count") / F.col("capacity")) \
     .withColumn("timestamp", F.current_timestamp())

    # ML
    ml_df = batch_df.select(
    "icao24", "speed_kmh", "altitude", "vertical_rate", "region", "timestamp"
    ).dropna().dropDuplicates(["icao24"])
    anomalies_pdf = None

    if ml_df.count() > 10:
        pdf = ml_df.toPandas()

        # Anomaly Detection
        anomalies_pdf = anomaly_model.detect(pdf)
        if not anomalies_pdf.empty:
            print(f"  Anomalies detected: {len(anomalies_pdf)}")
            anomalies_pdf["anomaly_score"] = anomalies_pdf["anomaly_score"].astype(float)
            spark.createDataFrame(
                anomalies_pdf[["icao24", "speed_kmh", "altitude", "vertical_rate", "anomaly_score"]]
            ).withColumn("timestamp", F.current_timestamp()) \
             .withColumn("reason", F.lit("IsolationForest anomaly")) \
             .write.format("org.apache.spark.sql.cassandra") \
             .mode("append").options(table="anomaly_logs", keyspace="airspace").save()
        else:
            print("  No anomalies this batch.")

        # Clustering
        clustered = cluster_model.cluster(pdf)
        print(f"  Flight clusters: {clustered['cluster'].value_counts().to_dict()}")

        # Forecasting
        forecast = forecaster.forecast(pdf)
        print(f"  Forecast: {sorted(forecast.items(), key=lambda x: -x[1])[:3]}")

    # Optimization
    region_metrics_pdf = region_metrics.toPandas()
    region_aci_dict = dict(zip(region_metrics_pdf["region"], region_metrics_pdf["aci"]))

    batch_pdf = batch_df.select("icao24", "speed_kmh", "altitude").dropna().toPandas()
    if not batch_pdf.empty:
        scored = optimizer.efficiency_score(batch_pdf)
        print(f"  Avg efficiency score: {round(scored['efficiency_score'].mean(), 1)}/100")

    suggestions = optimizer.reroute_suggestions(region_aci_dict)
    for s in suggestions[:2]:
        print(f"  Reroute -> {s}")

    overloaded = [r for r, s in optimizer.load_balance(region_aci_dict).items() if s == "OVERLOADED"]
    if overloaded:
        print(f"  Overloaded regions: {overloaded[:3]}")

    # Alerts
    all_alerts = alert_engine.congestion_alerts(region_metrics_pdf)
    if anomalies_pdf is not None and not anomalies_pdf.empty:
        all_alerts += alert_engine.anomaly_alerts(anomalies_pdf)
    all_alerts += alert_engine.surge_alerts(region_count_dict)
    alert_engine.print_alerts(all_alerts)

    # Write alerts to Cassandra
    alerts_df = region_metrics \
        .withColumn("alert",
            F.when(F.col("aci") > 1.2, "HIGH_CONGESTION")
            .when(F.col("aci") > 0.8, "MEDIUM_CONGESTION")
        ).filter(F.col("alert").isNotNull()) \
         .withColumn("timestamp", F.current_timestamp())

    if alerts_df.count() > 0:
        alerts_df.select(
            F.col("region"),
            F.col("timestamp"),
            F.col("alert").alias("alert_type")
        ).write.format("org.apache.spark.sql.cassandra") \
         .mode("append").options(table="alerts", keyspace="airspace").save()

    # Write to Cassandra
    batch_df.select(
        F.col("icao24"),
        F.col("event_time").alias("timestamp"),
        F.col("altitude"), F.col("callsign"), F.col("country"),
        F.col("latitude"), F.col("longitude"),
        F.col("velocity"), F.col("vertical_rate")
    ).write.format("org.apache.spark.sql.cassandra") \
     .mode("append").options(table="flight_events", keyspace="airspace").save()

    region_metrics.write.format("org.apache.spark.sql.cassandra") \
        .mode("append").options(table="region_metrics", keyspace="airspace").save()

    country_metrics.write.format("org.apache.spark.sql.cassandra") \
        .mode("append").options(table="country_metrics", keyspace="airspace").save()

    print(f"  ✅ Batch {batch_id} complete.")


query = df_final.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()