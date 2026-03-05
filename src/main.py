from extract import extract_and_load_bronze
from transform import process_bronze_to_silver
from load import load_gold_to_bigquery
from src.utils.logging_config import get_logger
from datetime import date


def main():
    file_name = "coins_market"
    target_date = date.today()
    logger = get_logger(__name__)
    logger.info("Starting ETL")

    try:
        logger.info("Starting data extraction")
        extract_and_load_bronze(file_name, target_date)

        logger.info("Starting data transformation")
        process_bronze_to_silver(target_date, file_name)

        logger.info("Starting data loading")
        load_gold_to_bigquery(target_date)
    except Exception as e:
        logger.error(f"An error occurred at ETL: {e}")


if __name__ == "__main__":
    main()
