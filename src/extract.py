from requests.adapters import HTTPAdapter, Retry
from datetime import date
from config import get_bucket
from logging_config import get_logger
import requests
import json
import os

logger = get_logger(__name__)


def get_api_data(url, headers, total_retries: int = 5):
    s = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=1,
        status_forcelist=[408, 425, 429, 500, 502, 503, 504],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        logger.info(f"Trying to get API data from {url}")
        response = s.get(url, headers=headers, timeout=30)

        response.raise_for_status()

        logger.info("Sucessfully fetched API data")

        return response

    except requests.exceptions.HTTPError as e:
        logger.error(
            f"HTTP error (Status {response.status_code}) while accessing {url}: {e}"
        )
        raise

    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: Couldn't reach API in {url}: {e}")
        raise

    except Exception as e:
        logger.error(f"API unexpected error : {e}")
        raise


def load_raw_data(response, file_name, reference_date: date):
    bucket = get_bucket("crypto-prj-bucket")

    logger.info(f"Starting {file_name} load into bronze storage bucket")
    load_file_path = f"bronze/{reference_date}/{file_name}.json"
    blob = bucket.blob(load_file_path)

    try:
        logger.info(f"Loading blob into bucket")
        blob.upload_from_string(
            response.text, content_type=response.headers.get("Content-type")
        )

    except Exception as e:
        logger.error(f"Unexpected error while processing {blob.name}: {e}")
        raise


def extract_and_load_bronze(file_name, reference_date):
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    api_key = os.getenv("API_KEY")
    headers = {"x-cg-demo-api-key": api_key}
    response = get_api_data(url, headers)
    load_raw_data(response, file_name, reference_date)


if __name__ == "__main__":
    extract_and_load_bronze("coins_market")
