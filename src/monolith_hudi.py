from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from common.schema import *
from src.common.params import *


def read_from_kafka(spark_session):
    return spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", kafka_topic) \
        .load()


def add_schema(spark_session, schema):
    raw_df = read_from_kafka(spark_session)
    df_with_schema = raw_df \
        .select(from_json(col("value").cast(StringType()), schema).alias("data")) \
        .select("data.*")

    return df_with_schema


def change_col_names(spark_session, schema):
    df = add_schema(spark_session, schema)
    col_name_df = df.select(
        col("c").cast(StringType()).alias("trade_code"),
        col("s").cast(StringType()).alias("symbol"),
        col("p").cast(DoubleType()).alias("last_price"),
        col("v").cast(DoubleType()).alias("volume"),
        col("t").cast(LongType()).alias("timestamp")
    )

    return col_name_df


def change_timestamp_type(spark_session, schema):
    df = change_col_names(spark_session, schema)
    secs_ts_df = df.withColumn("timestamp_seconds", col("timestamp") / 1000)
    transaction_time_df = secs_ts_df \
        .withColumn("transaction_time", from_unixtime(col("timestamp_seconds"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("timestamp", "timestamp_seconds", "trade_code")

    return transaction_time_df


def retrieve_ticker(spark_session, schema):
    df = change_timestamp_type(spark_session, schema)
    ticker_df = df \
        .withColumn("ticker", regexp_replace(col("symbol"), "BINANCE:|USDT", "")) \
        .select(col("ticker"), col("transaction_time"), col("last_price"), col("volume"))

    return ticker_df


def add_uuid(spark_session, schema):
    df = retrieve_ticker(spark_session, schema)
    df_with_uuid = df.withColumn("uuid", expr("uuid()"))

    return df_with_uuid


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Monolith Application") \
        .master("local[2]") \
        .getOrCreate()

    add_uuid(spark, finnhub_schema) \
        .writeStream \
        .format("hudi") \
        .options(**hudi_options) \
        .outputMode("append") \
        .option("path", f"s3a://{bucket_name}/monolith/",) \
        .option("checkpointLocation", f"s3a://{bucket_name}/monolith-checkpoint/") \
        .start() \
        .awaitTermination()
