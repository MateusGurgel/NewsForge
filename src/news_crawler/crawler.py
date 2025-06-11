import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
from botocore.client import Config
from hashlib import sha256
from news_crawler.env import INGESTION_S3_ACCESS_KEY, INGESTION_S3_SECRET_KEY, S3_ENDPOINT, S3_BUCKET
import boto3

visited = set()

def send_html_to_s3(html: str):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=INGESTION_S3_ACCESS_KEY,
        aws_secret_access_key=INGESTION_S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version='s3v4'),
    )

    filename=sha256(html.encode('utf-8')).hexdigest() + ".html"

    s3.put_object(Bucket=S3_BUCKET, Key=f"unprocessed/{filename}", Body=html.encode('utf-8'))

def crawl(url, depth=1):
    if depth == 0 or url in visited:
        return

    try:
        response = requests.get(url)
        visited.add(url)
    except requests.RequestException as e:
        print(f"Erro ao acessar {url}: {e}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')

    if re.search(r"noticia", url):
        print(f"🛍️ Baixando: {url}")
        html = requests.get(url).text
        send_html_to_s3(html)

    for link_tag in soup.find_all('a', href=True):
        href = link_tag['href']
        full_url = urljoin(url, href)

        if full_url.startswith('http'):
            crawl(full_url, depth - 1)

crawl('https://g1.globo.com/', 2)