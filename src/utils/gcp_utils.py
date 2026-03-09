from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.cloud_logging import CloudLoggingHook
import google.cloud.logging


def get_bucket(bucket_name) -> GCSHook:
    hook = GCSHook(gcp_conn_id='google_cloud_default')
    return hook.get_conn().bucket(bucket_name)


def get_bq_client() -> BigQueryHook:
    hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    return hook.get_client()  


def get_logging_client() -> CloudLoggingHook:
    hook = CloudLoggingHook(gcp_conn_id='google_cloud_default')
    client = google.cloud.logging.Client(
        project = hook.project_id,
        credentials = hook.get_credentials()
    )
    return client