from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.common.params import *
from src.common.utils import *
from src.common.schema import *


def write_to_s3_bronze(spark_session, schema, bucket_name, source_zone, target_zone, output_mode):
    df_with_schema = read_from_s3(spark_session, schema, bucket_name, source_zone)

    bronze_df = df_with_schema.select(
        col("c").cast(StringType()).alias("trade_code"),
        col("s").cast(StringType()).alias("symbol"),
        col("p").cast(DoubleType()).alias("last_price"),
        col("v").cast(DoubleType()).alias("volume"),
        col("t").cast(LongType()).alias("timestamp")
    )

    write_to_s3(bronze_df, bucket_name, target_zone, output_mode)
    # write_to_console_append(bronze_df)


if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Bronze Data Loader") \
        .config("spark.jars.packages", spark_jar_packages) \
        .config("fs.s3a.aws.credentials.provider", aws_credentials_provider) \
        .master("local[2]") \
        .getOrCreate()

    write_to_s3_bronze(spark, finnhub_schema, "finnhub-data-pipeline", "raw", "bronze", "append")
