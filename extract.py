from requests.adapters import HTTPAdapter, Retry
from datetime import date
from config import get_bucket, get_gcp_auth
import requests
import json
import os

def get_api_data(url, headers, file_name, local_dir, total_retries : int = 5):
   file_path = os.path.join(local_dir, file_name)
   s = requests.Session()

   retries = Retry(total=total_retries, 
                   backoff_factor=1, 
                   status_forcelist=[408, 425, 429, 500, 502, 503, 504])
   s.mount('https://', HTTPAdapter(max_retries=retries))

   try:
      response = s.get(url, headers=headers, timeout=30)
      
      response.raise_for_status()

      with open(file=f'{file_path}.json', mode='w') as file:
         json.dump(response.json(), fp=file, indent=4)
      
   except requests.exceptions.HTTPError as e:
      print(f'HTTP error (Status {response.status_code}) while accessing {url}: {e}')
      raise

   except requests.exceptions.ConnectionError as e:
      print(f'Connection error: Couldn\'t reach API in {url}: {e}')
      raise

   except Exception as e:
      print(f'API unexpected error : {e}')
      raise

# def get_coins_ids(headers):
#    url = 'https://api.coingecko.com/api/v3/coins/list'

#    get_api_data(url, headers, 'coins_ids')

# def get_coins_market_cap(headers):
#    url = 'https://api.coingecko.com/api/v3/global'

#    get_api_data(url, headers, 'coins_market_cap')

def get_coins_market(headers, local_dir):
      
   url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

   get_api_data(url, headers, 'coins_market', local_dir)

def load_raw_data(local_dir):
   bucket = get_bucket('crypto-prj-bucket')

   for file in os.listdir(local_dir):
      local_file_path = os.path.join(local_dir, file)
      load_file_path = f'bronze/{date.today()}/{file}'
      blob = bucket.blob(load_file_path)

      try:
         if blob.exists():
            blob.delete()

         with open(local_file_path) as f:
            blob.upload_from_string(f.read())

      except FileNotFoundError:
         print(f'File not found at {local_file_path}')

      except PermissionError:
         print(f'No permision to read file {local_file_path}')

      except Exception as e:
         print(f'Unexpected error while processing {blob}')

if __name__ == '__main__':
   local_dir = 'data/bronze'
   os.makedirs(local_dir, exist_ok=True)
   api_key = os.getenv('API_KEY')
   headers = {'x-cg-demo-api-key' : api_key}
   get_coins_market(headers, local_dir)
   load_raw_data(local_dir)
