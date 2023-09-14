import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.common.schema import finnhub_schema

ssm_client = boto3.client("ssm")
bucket_name = ssm_client.get_parameter(Name="finnhub-bucket-name")["Parameter"]["Value"]
bootstrap_server = ssm_client.get_parameter(Name="finnhub-bootstrap-server")["Parameter"]["Value"]
kafka_topic = ssm_client.get_parameter(Name="finnhub-kafka-topic")["Parameter"]["Value"]

def read_from_kafka(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "finnhub") \
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
        .config("spark.jars", "/Users/sagarl/dependencies/pyspark/spark-sql-kafka-0-10_2.12-3.3.1.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1") \
        .master("local[2]") \
        .getOrCreate()

    write_raw_to_s3(spark)
