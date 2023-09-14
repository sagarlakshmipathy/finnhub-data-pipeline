import boto3

sm_client = boto3.client("secretsmanager")
ssm_client = boto3.client("ssm")

api_key = sm_client.get_secret_value(SecretId="finnhub-api-key")["SecretString"]
kafka_topic = ssm_client.get_parameter(Name="finnhub-kafka-topic")["Parameter"]["Value"]
bootstrap_server = ssm_client.get_parameter(Name="finnhub-bootstrap-server")["Parameter"]["Value"]
bucket_name = ssm_client.get_parameter(Name="finnhub-bucket-name")["Parameter"]["Value"]
spark_kafka_jar_path = "/Users/sagarl/dependencies/pyspark/spark-sql-kafka-0-10_2.12-3.3.1.jar"
spark_jar_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.1"
