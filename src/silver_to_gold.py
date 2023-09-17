from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.common.params import *
from src.common.utils import *
from src.common.schema import *


def aggregate_volume(spark_session, bucket_name, zone):
    static_df = spark_session.read \
        .format("json") \
        .option("inferSchema", True) \
        .load(f"s3a://{bucket_name}/{zone}/")
    static_schema = static_df.schema
    silver_df = read_from_s3(spark_session, static_schema, bucket_name, zone)
    hourly_window_df = silver_df \
        .groupBy(col("ticker"), window(col("transaction_time"), "10 seconds").alias("time")) \
        .agg(
            round(sum(col("volume")), 2).alias("total_volume"),
            round(avg(col("last_price")), 3).alias("avg_price")) \
        .select(
            col("ticker"),
            col("time.start").alias("start"),
            col("time.end").alias("end"),
            col("avg_price"),
            col("total_volume")) \
        .orderBy(col("start").desc_nulls_last())

    # write_to_console(hourly_window_df, "complete")

    # .withWatermark(to_timestamp("transaction_time"), "2 seconds") \

    # write_to_s3(hourly_window_df, bucket_name, "gold", "org.apache.hudi", "complete")
    return hourly_window_df


def write_to_s3_gold(spark_session, bucket_name, source_zone, target_zone, format, output_mode):
    df = aggregate_volume(spark_session, bucket_name, source_zone)
    amount_spent_df = df \
        .withColumn("total_amount", round(col("avg_price") * col("total_volume"), 2))

    write_to_s3(amount_spent_df, bucket_name, target_zone, format, output_mode)
    # write_to_console_complete(amount_spent_df)

    # Specify common DataSourceWriteOptions in the single hudiOptions variable
    # hudi_options = {
    #     'hoodie.table.name': 'finnhub'
    # }

    # Write a DataFrame as a Hudi dataset
    # writer = amount_spent_df.writeStream \
    #     .format('org.apache.hudi') \
    #     .options(**hudi_options) \
    #     .outputMode('append') \
    #
    # writer.start(f's3://{bucket_name}/{target_zone}/')


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Gold Data Loader") \
        .config("spark.jars.packages", spark_jar_packages) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", False) \
        .config("fs.s3a.aws.credentials.provider", aws_credentials_provider) \
        .master("local[2]") \
        .getOrCreate()

    write_to_s3_gold(spark, "finnhub-data-pipeline", "silver", "gold", "csv", "complete")
    # write_to_s3_gold(spark, "finnhub-data-pipeline", "silver")
