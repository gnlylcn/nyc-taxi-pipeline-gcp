import argparse
from pyspark.sql import functions as F

from src.common.spark import build_spark
from src.common.ids import with_trip_id


def parse_months(s: str):
    return [m.strip() for m in s.split(",") if m.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", required=True, help="Comma-separated months like 2023-01,2023-02")
    ap.add_argument("--raw-dir", required=True, help="GCS raw base path, e.g. gs://bucket/raw")
    ap.add_argument("--bronze-dir", required=True, help="GCS bronze output path, e.g. gs://bucket/bronze/nyc_taxi/yellow")
    ap.add_argument("--zone-path", required=True, help="GCS path to taxi_zone_lookup.csv (used for validation)")
    args = ap.parse_args()

    spark = build_spark("nyc-taxi-bronze")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    months = parse_months(args.months)

    _ = spark.read.option("header", True).csv(args.zone_path)

    out_path = args.bronze_dir.rstrip("/")

    for m in months:
        fname = f"yellow_tripdata_{m}.parquet"
        src_path = f"{args.raw_dir.rstrip('/')}/yellow/{fname}"
        print(f"[INFO] Reading raw month={m} from {src_path}")

        df = spark.read.parquet(src_path)

        df = (df
              .withColumn("source_month", F.lit(m))
              .withColumn("passenger_count", F.col("passenger_count").cast("double"))
              .withColumn("trip_distance", F.col("trip_distance").cast("double"))
              .withColumn("fare_amount", F.col("fare_amount").cast("double"))
              .withColumn("total_amount", F.col("total_amount").cast("double"))
              )

        df = with_trip_id(df)

        print(f"[INFO] month={m} writing -> {out_path} (overwrite partition source_month={m})")

        (df.write
           .mode("overwrite")
           .partitionBy("source_month")
           .parquet(out_path))

    print("[INFO] Bronze ingestion done.")


if __name__ == "__main__":
    main()
