from prefect.server.schemas.schedules import CronSchedule
from pyspark.sql import Row
from prefect import flow, task
from pyspark.sql import SparkSession
from newsforge.env import S3_ENDPOINT, BUCKET, DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD
from pyspark.sql.types import StructType, StructField, StringType


def extract_data(html_news: str):
    # Essa parte será feita posteriormente, pois ainda não decidir se vou usar LLMs para o processamento, ou só BFS.

    print("Noticia ingerida: ", html_news)
    return Row(title="Exemplo de notícia", origin="Example Origin", resume="Example Resume", transcription="Example Transcription")

@task()
def transform_data():

    print("Iniciando ETL")

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

    print("Spark Session Iniciada")

    rdd = spark.sparkContext.wholeTextFiles(f"s3a://{BUCKET}/*.html")

    htmls = rdd.toDF(["path", "value"])

    dados = htmls.rdd.map(lambda row: extract_data(row.value))

    schema = StructType([
        StructField("title", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("resume", StringType(), True),
        StructField("transcription", StringType(), True)
    ])

    df = spark.createDataFrame(dados, schema)

    df.write \
        .format("jdbc") \
        .option("url", DATABASE_URL) \
        .option("user", DATABASE_USER) \
        .option("password", DATABASE_PASSWORD) \
        .option("dbtable", "news") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()

@flow(name="ETL HTML News")
def batch():
    transform_data()


if __name__ == "__main__":
    print(batch())