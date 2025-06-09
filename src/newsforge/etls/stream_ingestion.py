from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, count, approx_count_distinct

from newsforge.env import S3_ENDPOINT, BUCKET

@task()
def info_stream():
    spark = SparkSession.builder \
        .appName("ProcessHTMLS3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.proxy.host", "127.0.0.1") \
        .config("spark.hadoop.fs.s3a.proxy.port", "9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    df = spark.readStream \
        .format("TEXT") \
        .load(f"s3a://{BUCKET}/unprocessed/")

    df_with_file = df.withColumn("file", input_file_name())

    result = df_with_file.agg(
        approx_count_distinct("file").alias("files_read"),
        count("file").alias("lines_read")
    )

    query = result.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", f"s3a://{BUCKET}/checkpoints/s3-stream/") \
        .start()

    query.awaitTermination()


@flow(name="STREAM INGESTION")
def stream_ingestion():
    info_stream()

if __name__ == "__main__":
    stream_ingestion()