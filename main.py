from extract import extract_and_load_bronze
from transform import process_bronze_to_silver
from load import load_gold_to_bigquery
from dotenv import load_dotenv
from logging_config import get_logger
import os
from datetime import date

def main():
    load_dotenv()
    project_id = os.getenv('GCP_PROJECT_ID')
    file_name = "coins_market"
    target_date = date(2026, 2, 11)
    logger = get_logger(__name__)
    logger.info('Starting ETL')
   
    try:
        logger.info('Starting data extraction')
        extract_and_load_bronze(file_name, target_date)

        logger.info('Starting data transformation')
        process_bronze_to_silver(target_date,file_name)

        logger.info('Starting data loading')
        load_gold_to_bigquery("./sql/", project_id)
    except Exception as e:
        logger.error(f'An error occurred at ETL: {e}')

if __name__ == '__main__':
    main()