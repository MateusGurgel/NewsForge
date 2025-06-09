from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, count, approx_count_distinct

from newsforge.utils.env import S3_ENDPOINT, S3_BUCKET, S3_PROXY_HOST, S3_PROXY_PORT, KAFKA_BROKER, BATCH_S3_SECRET_KEY, BATCH_S3_ACCESS_KEY


@task()
def info_stream():
    spark = SparkSession.builder \
        .appName("ProcessHTMLS3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.proxy.host", S3_PROXY_HOST) \
        .config("spark.hadoop.fs.s3a.proxy.port", S3_PROXY_PORT) \
        .config("spark.hadoop.fs.s3a.secret.key", BATCH_S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.access.key", BATCH_S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .getOrCreate()

    df = spark.readStream \
        .format("TEXT") \
        .load(f"s3a://{S3_BUCKET}/unprocessed/")

    df_with_file = df.withColumn("file", input_file_name())

    result = df_with_file.agg(
        approx_count_distinct("file").alias("files_read"),
        count("file").alias("lines_read")
    )

    result_to_send = result.selectExpr(
        "CAST(null AS STRING) AS key",
        "to_json(struct(files_read, lines_read)) AS value"
    )

    query = result_to_send.writeStream \
        .format("kafka") \
        .option("topic", "info-news") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("checkpointLocation", f"s3a://{S3_BUCKET}/checkpoints/s3-stream/") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()


@flow(name="STREAM INGESTION")
def stream_ingestion():
    info_stream()

if __name__ == "__main__":
    stream_ingestion()