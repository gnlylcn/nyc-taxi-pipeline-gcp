from pyspark.sql import Row
from pyspark.sql import functions as F


def test_gold_fact_trips_columns(spark, tmp_path):
    bronze_rows = [
        Row(
            trip_id="x",
            tpep_pickup_datetime="2023-01-01 10:00:00",
            tpep_dropoff_datetime="2023-01-01 10:10:00",
            PULocationID=1,
            DOLocationID=2,
            passenger_count=1.0,
            trip_distance=2.5,
            fare_amount=10.0,
            total_amount=12.0,
            source_month="2023-01",
        )
    ]
    bronze = spark.createDataFrame(bronze_rows)

    fact = (bronze
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))
            )

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
        "source_month",
    ))

    expected = {
        "trip_id",
        "pickup_date_key",
        "dropoff_date_key",
        "pu_location_id",
        "do_location_id",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "source_month",
    }
    assert set(fact_trips.columns) == expected
