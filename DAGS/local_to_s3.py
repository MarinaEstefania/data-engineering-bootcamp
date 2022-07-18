from fileinput import filename
import logging
import os
from venv import create

from airflow import models
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

#The following examples of OS environment variables used to pass arguments to the operator:
S3_BUCKET = os.environ.get("S3_BUCKET", "name-of-the-test-bucket")
S3_KEY = os.environ.get("S3_KEY", "my-key")


file_path = " "
def find_path():
    global file_path
    # CSV loading to table.
    # Getting the current work directory (cwd)
    table_dir = os.getcwd()
    logging.info("table dir = " + table_dir)
    # r=root, d=directories, f=files
    for r, d, f in os.walk(table_dir):
        for file in f:
            if file.endswith("log_reviews.csv"):
                table_path = os.path.join(r, file)
    logging.info("table path = " + table_path)
    file_path = table_path

with models.DAG(
    "upload_local_to_s3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    find_relative_path = PythonOperator(
        task_id = "find_relative_path",
        python_callable = find_path
    )
    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=file_path,
        dest_key=S3_KEY,
        dest_bucket=S3_BUCKET,
        replace=True,
    )

    find_relative_path >> create_local_to_s3_job
    