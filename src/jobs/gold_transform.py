import argparse
from pyspark.sql import functions as F

from src.common.spark import build_spark

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-dir", required=True)
    ap.add_argument("--gold-dir", required=True)
    ap.add_argument("--zone-path", required=True)
    args = ap.parse_args()

    spark = build_spark("nyc-taxi-gold")

    bronze = spark.read.parquet(args.bronze_dir)

    zones = (spark.read.option("header", True).csv(args.zone_path)
             .select(
                 F.col("LocationID").cast("int").alias("location_id"),
                 F.col("Borough").alias("borough"),
                 F.col("Zone").alias("zone"),
                 F.col("service_zone").alias("service_zone")
             ))

    fact = (bronze
            .filter(F.col("tpep_pickup_datetime").isNotNull())
            .filter(F.col("tpep_dropoff_datetime").isNotNull())
            .filter(F.col("trip_distance") >= 0)
            .filter(F.col("total_amount").isNotNull())
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))
            )

    dim_date = (fact.select(F.col("pickup_date").alias("date_key"))
                .unionByName(fact.select(F.col("dropoff_date").alias("date_key")))
                .filter(F.col("date_key").isNotNull())
                .distinct()
                .withColumn("year", F.year("date_key"))
                .withColumn("month", F.month("date_key"))
                .withColumn("day", F.dayofmonth("date_key"))
                )

    dim_zone = zones.dropDuplicates(["location_id"])

    fact_trips = (fact.select(
        "trip_id",
        F.col("pickup_date").alias("pickup_date_key"),
        F.col("dropoff_date").alias("dropoff_date_key"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "source_month"
    ))

    gold = args.gold_dir.rstrip("/")
    print(f"[INFO] Writing gold tables -> {gold}")

    (dim_date.write.mode("overwrite").parquet(f"{gold}/dim_date"))
    (dim_zone.write.mode("overwrite").parquet(f"{gold}/dim_zone"))
    (fact_trips.write.mode("overwrite").parquet(f"{gold}/fact_trips"))

    print("[INFO] Gold transform done.")

if __name__ == "__main__":
    main()
