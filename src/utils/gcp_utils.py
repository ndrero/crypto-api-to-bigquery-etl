from google.cloud import storage, bigquery, logging
from google.oauth2 import service_account
from utils.config import PROJECT_ID, CREDENTIALS_PATH


def get_credentials():
    return service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)


def get_bucket(bucket_name):
    client = storage.Client(project=PROJECT_ID, credentials=get_credentials())
    return client.bucket(bucket_name)


def get_bq_client():
    return bigquery.Client(project=PROJECT_ID, credentials=get_credentials())


def get_logging_client():
    return logging.Client(credentials=get_credentials())
