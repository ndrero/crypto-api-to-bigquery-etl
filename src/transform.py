import pandas as pd
import json
import datetime as dt
import numpy as np
from utils.gcp_utils import get_bucket
from utils.logging_config import get_logger
from decimal import Decimal, ROUND_HALF_UP
from utils.config import BUCKET_NAME, DECIMAL_COLUMNS, COLUMNS_MAPPING, TIMESTAMP_COLUMNS
from utils.schema import pq_schema

logger = get_logger(__name__)

def to_decimal(x, places = -9):
    if pd.isna(x): 
        return None
    quantizer = Decimal(10) ** places
    return Decimal(str(x)).quantize(quantizer, rounding=ROUND_HALF_UP)

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

        json_data = json.loads(data, parse_float=Decimal)
        df = pd.json_normalize(json_data, sep="_")

        logger.info(f"Dataframe with {df.shape[0]} rows and {df.shape[1]} columns")

        df = df.rename(columns=COLUMNS_MAPPING)

        initial_rows = df.shape[0]
        df = df.drop_duplicates()
        if df.shape[0] < initial_rows:
            logger.info(f"Dropped {initial_rows - df.shape[0]} duplicate rows")

        for col in DECIMAL_COLUMNS:
            df[col] = df[col].apply(lambda x: to_decimal(x))

        df[TIMESTAMP_COLUMNS] = df[TIMESTAMP_COLUMNS].apply(pd.to_datetime, errors="coerce", utc=True)

        df["ingested_at"] = dt.datetime.now(tz=dt.timezone.utc)

        df["reference_dt"] = pd.to_datetime(target_date).date()

        logger.info(f"Dataframe transformation conclude. Final shape: {df.shape}")

        output_path = f"gcs://{BUCKET_NAME}/silver/crypto_market/"
        logger.info(f"Loading as parquet to {output_path}")
        
        df.to_parquet(
            f"gs://{BUCKET_NAME}/silver/crypto_market/{target_date}.parquet",
            engine="pyarrow",
            index=False,
            schema=pq_schema
        )

        logger.info("Silver layer load complete.")

    except Exception as e:
        logger.error(
            f"Critical failure processing {target_date}: {str(e)}", exc_info=True
        )
        raise e


if __name__ == "__main__":
    process_bronze_to_silver(dt.date.today(), "coins_market")
