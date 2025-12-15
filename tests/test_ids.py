from pyspark.sql import Row
from src.common.ids import with_trip_id


def test_trip_id_is_deterministic(spark):
    rows = [
        Row(
            tpep_pickup_datetime="2023-01-01 10:00:00",
            tpep_dropoff_datetime="2023-01-01 10:10:00",
            PULocationID=1,
            DOLocationID=2,
            passenger_count=1.0,
            trip_distance=2.5,
            fare_amount=10.0,
            total_amount=12.0,
        ),
        Row(
            tpep_pickup_datetime="2023-01-01 10:00:00",
            tpep_dropoff_datetime="2023-01-01 10:10:00",
            PULocationID=1,
            DOLocationID=2,
            passenger_count=1.0,
            trip_distance=2.5,
            fare_amount=10.0,
            total_amount=12.0,
        ),
    ]
    df = spark.createDataFrame(rows)

    a = with_trip_id(df).select("trip_id").collect()
    b = with_trip_id(df).select("trip_id").collect()

    assert [r.trip_id for r in a] == [r.trip_id for r in b]
    assert a[0].trip_id == a[1].trip_id
