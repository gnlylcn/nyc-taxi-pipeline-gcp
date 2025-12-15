from pyspark.sql import functions as F

def with_trip_id(df):
    # Deterministic trip_id from stable business columns (stringified).
    cols = [
        F.col("tpep_pickup_datetime").cast("string"),
        F.col("tpep_dropoff_datetime").cast("string"),
        F.col("PULocationID").cast("string"),
        F.col("DOLocationID").cast("string"),
        F.col("passenger_count").cast("string"),
        F.col("trip_distance").cast("string"),
        F.col("fare_amount").cast("string"),
        F.col("total_amount").cast("string"),
    ]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("||", *cols), 256))
