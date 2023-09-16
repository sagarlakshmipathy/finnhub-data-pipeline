import boto3

sm_client = boto3.client("secretsmanager")
ssm_client = boto3.client("ssm")

api_key = sm_client.get_secret_value(SecretId="finnhub-api-key")["SecretString"]
kafka_topic = ssm_client.get_parameter(Name="finnhub-kafka-topic")["Parameter"]["Value"]
bootstrap_server = ssm_client.get_parameter(Name="finnhub-bootstrap-server")["Parameter"]["Value"]
bucket_name = ssm_client.get_parameter(Name="finnhub-bucket-name")["Parameter"]["Value"]

# local testing
# local_output = "/Users/sagarl/IdeaProjects/finnhub-data-pipeline/experimentation/tmp/finnhub"
# base_streaming_path = "/Users/sagarl/IdeaProjects/finnhub-data-pipeline/experimentation/tmp/finnhub"
# local_checkpoint = "/Users/sagarl/IdeaProjects/finnhub-data-pipeline/experimentation/tmp/checkpoint/finnhub"

# jars and configs
# spark_jar_packages = (
#     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,"
#     "org.apache.hadoop:hadoop-aws:3.3.1,"
#     "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.3,"
#     "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1"
# )
#
# spark_serializer = "org.apache.spark.serializer.KryoSerializer"
# spark_sql_catalog_spark_catalog = "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
# spark_sql_extensions = "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"

table_name = "finnhub_data"

hudi_options = {
    "hoodie.upsert.shuffle.parallelism": 2,
    "hoodie.insert.shuffle.parallelism": 2,
    "hoodie.datasource.write.precombine.field": "transaction_time",
    "hoodie.datasource.write.recordkey.field": "uuid",
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.table.name": table_name,
    "hoodie.datasource.write.operation": "upsert"
}
