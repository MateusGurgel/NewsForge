from dotenv import load_dotenv
import os

load_dotenv()

# BATCH LAYER

INGESTION_S3_SECRET_KEY = os.getenv("BATCH_S3_SECRET_KEY")
INGESTION_S3_ACCESS_KEY = os.getenv("BATCH_S3_ACCESS_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")