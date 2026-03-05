import sys
sys.path.insert(0, '/opt/airflow')
sys.path.insert(0, '/opt/airflow/src')

from src.extract import extract_and_load_bronze
from src.transform import process_bronze_to_silver
from src.load import load_gold_to_bigquery
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
}

with DAG(
    'dag-crypto',
    default_args=default_args,
    description='A DAG to fetch cryptocurrency data',
    schedule="*/5 * * * *",
) as dag:
    
    @task
    def extract(ds):
        extract_and_load_bronze(file_name="coins_market", reference_date=ds)

    @task 
    def transform(ds):
        process_bronze_to_silver(target_date=ds, file_name="coins_market")

    @task 
    def load(ds):
        load_gold_to_bigquery(ds)

    extract("{{ ds }}") >> transform("{{ ds }}") >> load("{{ ds }}")