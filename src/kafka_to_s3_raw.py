from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.common.schema import *
from src.common.params import *
from src.common.utils import *


def read_from_kafka(spark_session):
    return spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", kafka_topic) \
        .load()


def write_to_s3_raw(spark_session, bucket_name):
    raw_df = read_from_kafka(spark_session)
    df_with_schema = raw_df \
        .select(from_json(col("value").cast(StringType()), finnhub_schema).alias("data")) \
        .select("data.*")

    write_to_s3(df_with_schema, bucket_name, "raw")
    # write_to_console_append(df_with_schema)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Raw Data Loader") \
        .config("spark.jars.packages", spark_jar_packages) \
        .config("fs.s3a.aws.credentials.provider", aws_credentials_provider) \
        .master("local[2]") \
        .getOrCreate()

    write_to_s3_raw(spark, "finnhub-data-pipeline")
