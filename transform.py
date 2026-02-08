import pandas as pd
import json
import datetime as dt
from config import get_bucket
from datetime import date


def process_bronze_to_silver(target_date, file_name) -> str:
    bucket = get_bucket("crypto-prj-bucket")
    blob = bucket.get_blob(f"bronze/{target_date}/{file_name}.json")
    data = blob.download_as_text()
    json_data = json.loads(data)
    df = pd.json_normalize(json_data)

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

    df["reference_dt"] = target_date

    df.to_parquet(
        "gcs://crypto-prj-bucket/silver/crypto_market/",
        partition_cols=["reference_dt"],
        engine="pyarrow",
    )


if __name__ == "__main__":
    process_bronze_to_silver("2026-02-04", "coins_market")
