from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago

DAG_ID = "csv_to_s3"

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id=DAG_ID,
    schedule_interval='@once',
    start_date=days_ago(1),
    catchup=False
) as dag:
    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='task_upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'C:/Users/mgarc/Documents/wizeline/bootcamp/data-engineering-bootcamp/DATA/movie_review.csv',
            'key': 'movie_review.csv',
            'bucket_name': 'myawsbucket-megc'
        }
    )

    task_upload_to_s3