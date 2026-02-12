import os
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

project_id = os.environ["GCP_PROJECT_ID"]
credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]


def get_credentials():
    return service_account.Credentials.from_service_account_file(credentials_path)


def get_bucket(bucket_name):
    client = storage.Client(project=project_id, credentials=get_credentials())
    return client.bucket(bucket_name)


def get_bq_client():
    return bigquery.Client(project=project_id, credentials=get_credentials())
