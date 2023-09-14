from pyspark.sql.types import *

finnhub_schema = StructType([
    StructField("c", StringType(), True),
    StructField("p", StringType(), True),
    StructField("s", StringType(), True),
    StructField("t", StringType(), True),
    StructField("v", StringType(), True)
])
