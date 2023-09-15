def read_from_s3(spark_session, schema, bucket_name, zone):
    df = spark_session.readStream \
        .format("json") \
        .schema(schema) \
        .load(f"s3a://{bucket_name}/{zone}/")
    return df


def write_to_s3(df, bucket_name, zone, format, output_mode):
    df.writeStream \
        .format(format) \
        .outputMode(output_mode) \
        .option("path", f"s3a://{bucket_name}/{zone}") \
        .option("checkpointLocation", f"s3a://{bucket_name}/{zone}-checkpoint") \
        .start() \
        .awaitTermination()


def write_to_console(df, output_mode):
    df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .start() \
        .awaitTermination()

