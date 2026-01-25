import requests
from requests.adapters import HTTPAdapter, Retry
from itertools import batched
import json
import os

def get_api_data(url, headers, file_name, total_retries : int = 5):
   file_path = os.path.join('data/raw', file_name)
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

def get_coins_market(headers):
      
   url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd'

   get_api_data(url, headers, 'coins_market')


if __name__ == '__main__':
   os.makedirs('data/raw', exist_ok=True)
   api_key = os.getenv('API_KEY')
   headers = {'x-cg-demo-api-key' : api_key}
   get_coins_market(headers)
