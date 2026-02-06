from requests.adapters import HTTPAdapter, Retry
from datetime import date
from config import get_bucket
from logging_config import get_logger
import requests
import json
import os

logger = get_logger(__name__)

def get_api_data(url, headers, file_name, local_dir, total_retries : int = 5):
   file_path = os.path.join(local_dir, file_name)
   s = requests.Session()

   retries = Retry(total=total_retries, 
                   backoff_factor=1, 
                   status_forcelist=[408, 425, 429, 500, 502, 503, 504])
   s.mount('https://', HTTPAdapter(max_retries=retries))

   try:
      logger.info(f'Trying to get API data from {url}')
      response = s.get(url, headers=headers, timeout=30)
      
      response.raise_for_status()

      logger.info('Sucessfully fetched API data')
      with open(file=f'{file_path}.json', mode='w') as file:
         logger.info(f'Loading data into {file_path}')
         json.dump(response.json(), fp=file, indent=4)
      
   except requests.exceptions.HTTPError as e:
      logger.error(f'HTTP error (Status {response.status_code}) while accessing {url}: {e}')
      raise

   except requests.exceptions.ConnectionError as e:
      logger.error(f'Connection error: Couldn\'t reach API in {url}: {e}')
      raise

   except Exception as e:
      logger.error(f'API unexpected error : {e}')
      raise

def get_coins_market(headers, local_dir):
      
   url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

   get_api_data(url, headers, 'coins_market', local_dir)

def load_raw_data(local_dir):
   bucket = get_bucket('crypto-prj-bucket')

   for file in os.listdir(local_dir):
      logger.info(f'Starting {file} load into bronze storage bucket')
      local_file_path = os.path.join(local_dir, file)
      load_file_path = f'bronze/{date.today()}/{file}'
      blob = bucket.blob(load_file_path)

      try:
         if blob.exists():
            logger.info(f'Deleting previous bronze storage blob')
            blob.delete()

         logger.info(f'Loading blob into bucket')
         blob.upload_from_filename(local_file_path)

      except FileNotFoundError:
         logger.error(f'File not found at {local_file_path}')

      except PermissionError:
         logger.error(f'No permision to read file {local_file_path}')

      except Exception as e:
         logger.error(f'Unexpected error while processing {blob.name}: {e}')
         raise

def extract_and_load_bronze(local_dir):
   os.makedirs(local_dir, exist_ok=True)
   api_key = os.getenv('API_KEY')
   headers = {'x-cg-demo-api-key' : api_key}
   get_coins_market(headers, local_dir)
   load_raw_data(local_dir)

if __name__ == '__main__':
   extract_and_load_bronze('data/bronze')
