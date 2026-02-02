from requests.adapters import HTTPAdapter, Retry
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import date
import requests
import json
import os

load_dotenv()

credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
project_id = os.environ['GCP_PROJECT_ID']

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
      
   except Exception as e:
      print(f'Error while getting API data: {e}')
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
   credentials = service_account.Credentials.from_service_account_file(credentials_path)
   client = storage.Client(project='crypto-etl-prj', credentials=credentials)
   bucket = client.bucket('crypto-prj-bucket')

   for file in os.listdir(local_dir):
      local_file_path = os.path.join(local_dir, file)
      load_file_path = f'bronze/{date.today()}/{file}'
      blob = bucket.blob(load_file_path)

      with open(local_file_path) as f:
         blob.upload_from_string(f.read())

if __name__ == '__main__':
   local_dir = 'data/bronze'
   os.makedirs(local_dir, exist_ok=True)
   api_key = os.getenv('API_KEY')
   headers = {'x-cg-demo-api-key' : api_key}
   get_coins_market(headers, local_dir)
   load_raw_data(local_dir)
