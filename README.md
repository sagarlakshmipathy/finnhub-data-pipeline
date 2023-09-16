# Finnhub Data Pipeline
This project is a data pipeline that retrieves real-time financial market data from the Finnhub API and processes it using Apache Kafka, Apache Spark and Apache Hudi. The pipeline consists of a Kafka producer, data transformations using Spark, using various parameters and configurations.
### Kafka Producer
The `producer.py` is responsible for connecting to the Finnhub WebSocket API, subscribing to specific symbols, and producing the received data to a Kafka topic. The producer utilizes the websocket and `confluent_kafka` library to establish a WebSocket connection and interact with Kafka.
### Transformations
The transformations module contains the data processing logic using Apache Spark. It includes functions for reading data from Kafka, adding schema to the raw data, changing column names and types, retrieving ticker information, and adding a UUID to each record. The reason UUID is added is to support saving the data in `hudi` table format. The transformed data is then written to a Hudi table.
### spark-submit command
The spark-submit command provided can be used to submit the Spark application to a cluster. It includes necessary package dependencies (`hadoop-aws`, `spark-sql-kafka`, `spark-streaming-kafka` and `hudi-spark`) and additional Spark configurations (`spark.serializer`, `spark.sql.catalog.spark_catalog`, `spark.sql.extensions`).
Please note that some parts of the code (commented out) are specific to local testing and may need modification for deployment in a production environment.
To run the pipeline, execute the pyspark command with the provided spark-submit command and necessary arguments.

**Note:** Make sure to install the required libraries (`websocket`, `confluent_kafka`, `pyspark`, `boto3`) before running the pipeline.

#### Command: 
```
sagarl@dev experimentation % spark-submit \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
--conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1,\
org.apache.hadoop:hadoop-aws:3.3.1,\
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,\
org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.3 \
monolith_hudi.py
```







