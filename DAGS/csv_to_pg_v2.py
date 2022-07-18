from asyncio import tasks
import logging
import os
from ssl import SSLSocket
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

DAG_ID = "csv_to_pg_v2"


S3_BUCKET = os.environ.get("S3_BUCKET", "test-bucket")
S3_KEY = os.environ.get("S3_KEY", "key")

def upload_data_func():
    s3_hook = S3Hook(s3_conn)
    local_filename = s3_hook.download_file(key=S3_KEY, bucket_name=S3_BUCKET)
    psql_hook = PostgresHook(pg_conn)
    psql_hook.copy_expert(sql = """COPY deb.user_purchase(
                invoice_number,
                stock_code,
                detail,
                quantity,
                invoice_date,
                unit_price,
                customer_id,
                country) 
                FROM STDIN
                DELIMITER ',' CSV HEADER;""", filename = local_filename)

def get_table_count():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute("SELECT COUNT(*) AS total_rows FROM deb.user_purchase")
    #cursor.close()
    #pg_conn.close
    logging.info("returning count: ")
    logging.info(cursor.fetchone())


with DAG(
    dag_id = DAG_ID, 
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False
) as dag:
    start_workflow = DummyOperator(task_id="start_worklow")
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="pg_conn", 
        sql="""
            CREATE SCHEMA if not exists deb;
            CREATE TABLE if not exists deb.user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            );
            """,
    )
    count = PythonOperator(
        task_id = "count",
        python_callable = get_table_count
    )
    upload_data = PythonOperator(
        task_id="upload_data",
        python_callable = upload_data_func
    )
    count_after_populate = PythonOperator(
        task_id = "count_after_populate",
        python_callable = get_table_count
    )
    end_workflow = DummyOperator(task_id="end_worklow")

    start_workflow >>  create_table >> count >> upload_data >> count_after_populate >> end_workflow