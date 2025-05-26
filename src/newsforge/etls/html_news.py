from pyspark.sql import Row
from prefect import flow, task
from newsforge.env import S3_ENDPOINT, PREFIX, BUCKET, S3_KEY, S3_PASSWORD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


def extract_data(html_news: str):
    print("Noticia ingerida: ", html_news)
    return Row(title="Exemplo de notícia", url="https://example.com", origin="Example Origin", resume="Example Resume", transcription="Example Transcription")

@task()
def transform_data():

    print("Iniciando ETL")

    spark = SparkSession.builder \
        .appName("ProcessHTMLS3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.access.key", "access_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "secret_key") \
        .config("spark.hadoop.fs.s3a.proxy.host", "minio") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.proxy.port", "9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    htmls = spark.read.text("s3a://meu-bucket/raw/*.html")

    dados = htmls.rdd.map(lambda row: extract_data(row.value))

    schema = StructType([
        StructField("titulo", StringType(), True),
        StructField("h1", StringType(), True)
    ])

    df = spark.createDataFrame(dados, schema)

    df.show()

    spark.stop()

@flow(name="ETL HTML News")
def batch():
    transform_data()


if __name__ == "__main__":
    print(batch())