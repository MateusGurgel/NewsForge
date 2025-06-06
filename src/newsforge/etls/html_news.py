from bs4 import BeautifulSoup
from pyspark.sql import Row
from prefect import flow, task
from pyspark.sql import SparkSession
from newsforge.env import S3_ENDPOINT, BUCKET, DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD
from pyspark.sql.types import StructType, StructField, StringType

from newsforge.models import gemini_flash
from newsforge.models.gemini_flash import GeminiFlash


def get_html_content(html_news: str) -> str:
    soup = BeautifulSoup(html_news, 'html.parser')

    tags = soup.find_all(['p', 'h1', 'h2', 'h3'])

    result = ""

    for tag in tags:
        result += tag.text.strip() + "\n"

    return result

def get_structured_data(html_news: str):
    pass

def extract_data(html_news: str):
    content_str: str = get_html_content(html_news)


    system_prompt: str = "You are a information extractor. Your task is to extract the required information from a news article."
    g_flash = GeminiFlash()
    result = g_flash.handle(system_prompt, content_str)

    return Row(title=result["title"], origin=result["news_origin"], resume=result["resume"], transcription=result["transcription"])

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

    df.show()

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