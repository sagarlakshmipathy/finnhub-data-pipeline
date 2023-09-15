from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.common.params import *
from src.common.utils import *
from src.common.schema import *


def change_timestamp_type(spark_session, bucket_name, zone):
    static_df = spark_session.read \
                    .format("json") \
                    .option("inferSchema", True) \
                    .load(f"s3a://{bucket_name}/{zone}/")
    static_schema = static_df.schema
    df = read_from_s3(spark_session, static_schema, bucket_name, zone)
    secs_ts_df = df.withColumn("timestamp_seconds", col("timestamp") / 1000)
    transaction_time_df = secs_ts_df \
        .withColumn("transaction_time", from_unixtime(col("timestamp_seconds"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("timestamp", "timestamp_seconds", "trade_code")

    return transaction_time_df


def write_to_s3_silver(spark_session, bucket_name, source_zone, target_zone, output_mode):
    df = change_timestamp_type(spark_session, bucket_name, source_zone)
    ticker_df = df \
        .withColumn("ticker", regexp_replace(col("symbol"), "BINANCE:|USDT", "")) \
        .select(col("ticker"), col("transaction_time"), col("last_price"), col("volume"))

    write_to_s3(ticker_df, bucket_name, target_zone, output_mode)
    # write_to_console_append(ticker_df)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Raw Data Loader") \
        .config("spark.jars.packages", spark_jar_packages) \
        .config("fs.s3a.aws.credentials.provider", aws_credentials_provider) \
        .master("local[2]") \
        .getOrCreate()

    write_to_s3_silver(spark, "finnhub-data-pipeline", "bronze", "silver", "append")
