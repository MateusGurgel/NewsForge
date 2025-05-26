from dotenv import load_dotenv
import os

load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET = os.getenv("BUCKET")
PREFIX = os.getenv("PREFIX")
S3_PASSWORD = os.getenv("S3_PASSWORD")
S3_KEY = os.getenv("S3_KEY")