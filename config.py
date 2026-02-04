import os
from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account

load_dotenv()

project_id = os.environ['GCP_PROJECT_ID']
credentials_path = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

def get_gcp_auth():
   credentials = service_account.Credentials.from_service_account_file(credentials_path)
   return storage.Client(project=project_id,credentials=credentials)

def get_bucket(bucket_name):
   client = get_gcp_auth()
   return client.bucket(bucket_name)