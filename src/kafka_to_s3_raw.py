from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.common.schema import *
from src.common.params import *


def read_from_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", kafka_topic) \
        .load()


def write_raw_to_s3(spark):
    raw_data = read_from_kafka(spark) \
        .select(from_json(col("value").cast(StringType()), finnhub_schema).alias("data")) \
        .select("data.*")

    raw_data.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", f"s3a://{bucket_name}/output") \
        .option("checkpointLocation", f"s3a://{bucket_name}/checkpoint") \
        .start() \
        .awaitTermination()


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Raw Data Loader") \
        .config("spark.jars.packages", spark_jar_packages) \
        .config("fs.s3a.aws.credentials.provider", aws_credentials_provider) \
        .master("local[2]") \
        .getOrCreate()

    write_raw_to_s3(spark)
