import os

from airflow import models
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.dates import datetime

S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")

with models.DAG(
    "upload_local_to_s3",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename="./../DATA/movie.review.csv",
        dest_key=S3_KEY,
        dest_bucket=S3_BUCKET,
        replace=True,
    )