from pyspark.sql import SparkSession

def build_spark(app_name: str) -> SparkSession:
    # Dataproc provides Spark runtime. Keep session config minimal.
    return SparkSession.builder.appName(app_name).getOrCreate()
