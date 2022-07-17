from fileinput import filename
import logging
import os

from airflow import models
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook

S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")

file=""
def find_path():
    # CSV loading to table.
    # Getting the current work directory (cwd)
    table_dir = os.getcwd()
    logging.info("table dir = " + table_dir)
    # r=root, d=directories, f=files
    for r, d, f in os.walk(table_dir):
        for file in f:
            if file.endswith("prueba.txt"):
                table_path = os.path.join(r, file)

    logging.info("table path = " + table_path)

    file = table_path

with models.DAG(
    "upload_local_to_s3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=file,
        dest_key=S3_KEY,
        dest_bucket=S3_BUCKET,
        replace=True,
    )