import requests
from requests.adapters import HTTPAdapter, Retry
import json
import os

def get_api_data(url, headers, file_name, total_retries : int = 5):
   s = requests.Session()

   retries = Retry(total=total_retries, 
                   backoff_factor=1, 
                   status_forcelist=[408, 425, 429, 500, 502, 503, 504])
   s.mount('https://', HTTPAdapter(max_retries=retries))

   try:
      response = s.get(url, headers=headers, timeout=30)
      
      response.raise_for_status()

      with open(file=f'{file_name}.json', mode='w') as file:
         json.dump(response.json(), fp=file, indent=4)
      
   except Exception as e:
      print(f'Error while getting API data: {e}')
      raise

def get_coins_ids(headers):
   url = 'https://api.coingecko.com/api/v3/coins/list'

   get_api_data(url, headers, 'coins_ids')

# def get_coins_price():
#    with open('coins_ids.json') as file:
#       coins = json.load(fp=file)

   # for coin in coins:
   #    coin_id = coin['id']

   # url = 'https://api.coingecko.com/api/v3/coins/list'
   # get_api_data(url, headers, 'coins_price')


if __name__ == '__main__':
   api_key = os.getenv('API_KEY')
   headers = {'x-cg-demo-api-key' : api_key}
   get_coins_ids(headers)

   # get_coins_price()




# response = requests.get('https://api.coingecko.com/api/v3/simple/price?vs_currencies=usd', headers=headers)