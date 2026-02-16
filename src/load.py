import os
from logging_config import get_logger
from gcp_utils import get_bq_client
from config import PROJECT_ID

logger = get_logger(__name__)


def load_gold_to_bigquery(sql_path, project_id):
    client = get_bq_client()

    os.makedirs(sql_path, exist_ok=True)
    sql_files = os.listdir(sql_path)
    if not sql_files:
        print("n tem arquivo")

    for file in sql_files:
        file_path = os.path.join(sql_path, file)
        with open(file_path, mode="r") as f:
            query = f.read()
        final_query = query.replace("@project_id", project_id)
        job = client.query(final_query)
        job.result()


if __name__ == "__main__":
    load_gold_to_bigquery("./sql/", PROJECT_ID)
