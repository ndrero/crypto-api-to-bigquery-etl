import os
from dotenv import load_dotenv

load_dotenv()

MARKET_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
API_KEY = os.getenv("API_KEY")
HEADERS = {"x-cg-demo-api-key": API_KEY}

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BUCKET_NAME = "crypto-prj-bucket"
