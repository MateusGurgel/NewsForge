from dotenv import load_dotenv
import os

load_dotenv()

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET = os.getenv("BUCKET")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")