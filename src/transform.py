import pandas as pd
import json
import datetime as dt
from gcp_utils import get_bucket
from logging_config import get_logger
from config import BUCKET_NAME

logger = get_logger(__name__)


def process_bronze_to_silver(target_date, file_name) -> str:
    try:
        logger.info(f"Starting Bronze to Silver process for date: {target_date}")
        bucket = get_bucket(BUCKET_NAME)

        blob_path = f"bronze/{target_date}/{file_name}.json"

        logger.debug(f"Reading file from: {blob_path}")
        blob = bucket.get_blob(blob_path)

        if not blob:
            logger.error(f"File not found at bucket path : {blob_path}")
            raise FileNotFoundError

        data = blob.download_as_text()
        logger.info(f"Successfully download data for {target_date}")

        json_data = json.loads(data)
        df = pd.json_normalize(json_data, sep="_")

        logger.info(f"Dataframe with {df.shape[0]} rows and {df.shape[1]} columns")
        mapping_columns = {
            "id": "coin_id",
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

        available_columns = [col for col in mapping_columns.keys() if col in df.columns]

        missing_columns = set(mapping_columns.keys()) - set(available_columns)

        if missing_columns:
            logger.info(f"Missing columns at response: {missing_columns}")

        df = df[available_columns].rename(columns=mapping_columns)

        initial_rows = df.shape[0]
        df = df.drop_duplicates()
        if df.shape[0] < initial_rows:
            logger.info(f"Dropped {initial_rows - df.shape[0]} duplicate rows")

        df["ingested_at"] = dt.datetime.now(tz=dt.timezone.utc)

        df["reference_dt"] = target_date

        logger.info(f"Dataframe transformation conclude. Final shape: {df.shape}")

        output_path = f"gcs://{BUCKET_NAME}/silver/crypto_market/"
        logger.info(f"Loading as parquet to {output_path}")
        df.to_parquet(
            output_path,
            partition_cols=["reference_dt"],
            engine="pyarrow",
            index=False,
        )
        logger.info("Silver layer load complete.")

    except Exception as e:
        logger.error(
            f"Critical failure processing {target_date}: {str(e)}", exc_info=True
        )
        raise e


if __name__ == "__main__":
    process_bronze_to_silver(dt.date.today(), "coins_market")
