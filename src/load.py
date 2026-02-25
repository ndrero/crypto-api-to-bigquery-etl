from google.cloud import bigquery
from logging_config import get_logger
from utils.gcp_utils import get_bq_client
from utils.config import PROJECT_ID, BUCKET_NAME, DATASET, TABLE_NAME
from datetime import date
from utils.schema import bq_schema

logger = get_logger(__name__)


def load_gold_to_bigquery(target_date):
    client = get_bq_client()

    gcs_uri = f"gs://{BUCKET_NAME}/silver/crypto_market/{target_date}.parquet"
    table_partition = f"{PROJECT_ID}.{DATASET}.{TABLE_NAME}${str(target_date).replace('-', '')}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema = bq_schema,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="reference_dt",
        ),
        clustering_fields=["coin_id"],
    )

    job = client.load_table_from_uri(
        gcs_uri,
        table_partition,
        job_config=job_config
    )
    job.result()


if __name__ == "__main__":
    load_gold_to_bigquery(date.today())
