from dotenv import load_dotenv
import os

load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET = os.getenv("BUCKET")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")