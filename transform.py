import pandas as pd
import json
import os
import datetime as dt
from datetime import date


def transform_data(bronze_path, silver_dir) -> str:
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"File not found: {bronze_path}")

    with open(bronze_path, mode="r") as file:
        coins = json.load(fp=file)

    df = pd.json_normalize(coins, sep="_")

    df = df.rename(
        columns={
            "id": "coin_id",
            "image": "image_url",
            "current_price": "current_price_usd",
            "market_cap": "market_cap_usd",
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
    )

    df["ingested_at"] = dt.datetime.now(tz=dt.timezone.utc)
    df["load_dt"] = date.today()

    df.to_parquet(silver_dir, partition_cols=["load_dt"], engine="pyarrow")

    return silver_dir


if __name__ == "__main__":
    transform_data("data/bronze/coins_market.json", "data/silver/coins_market/")
