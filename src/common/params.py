import boto3

sm_client = boto3.client("secretsmanager")
ssm_client = boto3.client("ssm")

api_key = sm_client.get_secret_value(SecretId="finnhub-api-key")["SecretString"]
kafka_topic = ssm_client.get_parameter(Name="finnhub-kafka-topic")["Parameter"]["Value"]
bootstrap_server = ssm_client.get_parameter(Name="finnhub-bootstrap-server")["Parameter"]["Value"]
bucket_name = ssm_client.get_parameter(Name="finnhub-bucket-name")["Parameter"]["Value"]
spark_jar_packages = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
    "org.apache.hadoop:hadoop-aws:3.3.1,"
    "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.1,"
    "org.apache.hudi:hudi-spark-bundle_2.12:0.13.1"
)
aws_credentials_provider = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
