import os
from dotenv import load_dotenv

load_dotenv()

MARKET_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
API_KEY = os.getenv("API_KEY")
HEADERS = {"x-cg-demo-api-key": API_KEY}

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

BUCKET_NAME = "crypto-prj-bucket"

DATASET = "crypto_gold"
TABLE_NAME = "obt_crypto_market"

COLUMNS_MAPPING = {
    "id": "coin_id",
    "current_price": "current_price_usd",
    "market_cap": "market_cap_usd",
    "market_cap_rank": "market_cap_rank",
    "fully_diluted_valuation": "fully_diluted_valuation_usd",
    "total_volume": "total_volume_usd",
    "high_24h": "high_24h_usd",
    "low_24h": "low_24h_usd",
    "price_change_24h": "price_change_24h_usd",
    "price_change_percentage_24h": "price_change_24h_pct",
    "market_cap_change_24h": "market_cap_change_24h_usd",
    "market_cap_change_percentage_24h": "market_cap_change_24h_pct",
    "circulating_supply": "circulating_supply",
    "total_supply": "total_supply",
    "max_supply": "max_supply",
    "ath": "all_time_high_usd",
    "ath_change_percentage": "all_time_high_change_pct",
    "ath_date": "all_time_high_date",
    "atl": "all_time_low_usd",
    "atl_change_percentage": "all_time_low_change_pct",
    "atl_date": "all_time_low_date",
    "roi_times": "return_on_investment_times",
    "roi_currency": "return_on_investment_currency",
    "roi_percentage": "return_on_investment_pct",
    "last_updated": "last_updated_at",
}

DECIMAL_COLUMNS = [
    "current_price_usd",
    "market_cap_usd",
    "fully_diluted_valuation_usd",
    "total_volume_usd",
    "high_24h_usd",
    "low_24h_usd",
    "price_change_24h_usd",
    "price_change_24h_pct",
    "market_cap_change_24h_usd",
    "market_cap_change_24h_pct",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "all_time_high_usd",
    "all_time_high_change_pct",
    "all_time_low_usd",
    "all_time_low_change_pct",
    "return_on_investment_times",
    "return_on_investment_pct",
]

TIMESTAMP_COLUMNS = ["all_time_high_date", "all_time_low_date", "last_updated_at"]
