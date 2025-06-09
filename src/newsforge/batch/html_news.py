import boto3
from bs4 import BeautifulSoup
from pyspark.sql import Row
from prefect import flow, task
from pyspark.sql import SparkSession
from newsforge.utils.env import S3_ENDPOINT, S3_BUCKET, BATCH_DATABASE_URL, BATCH_DATABASE_USER, BATCH_DATABASE_PASSWORD, \
    BATCH_S3_SECRET_KEY, BATCH_S3_ACCESS_KEY, S3_PROXY_HOST, S3_PROXY_PORT, SPEED_S3_SECRET_KEY, SPEED_S3_ACCESS_KEY
from pyspark.sql.types import StructType, StructField, StringType
from newsforge.utils.gemini_flash import GeminiFlash
from botocore.client import Config


def get_html_content(html_news: str) -> str:
    soup = BeautifulSoup(html_news, 'html.parser')

    tags = soup.find_all(['p', 'h1', 'h2', 'h3'])

    result = ""

    for tag in tags:
        result += tag.text.strip() + "\n"

    return result

def get_structured_data(html_news: str):
    system_prompt: str = "You are a information extractor. Your task is to extract the required information from a news article."
    g_flash = GeminiFlash()
    return g_flash.handle(system_prompt, html_news)

def extract_data(html_news: str):
    content_str: str = get_html_content(html_news)
    result = get_structured_data(content_str)

    return Row(title=result["title"], origin=result["news_origin"], resume=result["resume"], transcription=result["transcription"])

def list_files(s3, prefix=None, bucket=S3_BUCKET):
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    arquivos = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                arquivos.append(obj["Key"])
    return arquivos

@task()
def transform_data():

    print("Iniciando ETL")

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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    print("Spark Session Iniciada")

    rdd = spark.sparkContext.wholeTextFiles(f"s3a://{S3_BUCKET}/unprocessed/*.html")

    htmls = rdd.toDF(["path", "value"])

    dados = htmls.rdd.map(lambda row: extract_data(row.value))

    schema = StructType([
        StructField("title", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("resume", StringType(), True),
        StructField("transcription", StringType(), True)
    ])

    df = spark.createDataFrame(dados, schema)

    df.show()

    df.write \
        .format("jdbc") \
        .option("url", BATCH_DATABASE_URL) \
        .option("user", BATCH_DATABASE_USER) \
        .option("password", BATCH_DATABASE_PASSWORD) \
        .option("dbtable", "news") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()

@task()
def move_data_to_processed():

    s3 = boto3.client(
        "s3",
        aws_access_key_id=BATCH_S3_ACCESS_KEY,
        aws_secret_access_key=BATCH_S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version='s3v4'),
    )

    unprocessed_files: list[str] = list_files(s3, prefix="unprocessed/")

    if not unprocessed_files:
        print("Nenhum arquivo foi processado")
        return

    for file in unprocessed_files:
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource=f"{S3_BUCKET}/{file}",
            Key=file.replace("unprocessed/", "processed/"),
        )
        s3.delete_object(Bucket=S3_BUCKET, Key=file)

@flow(name="ETL HTML News")
def batch():
    try:
        transform_data()
        move_data_to_processed()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    print(batch())