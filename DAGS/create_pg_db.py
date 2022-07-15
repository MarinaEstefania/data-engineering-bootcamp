from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DAG_ID = "db_pg_ingestion"

get_table_count = PostgresOperator(
    task_id="get_table_count",
    postgres_conn_id="pg_db",
    sql="SELECT COUNT(*) AS total_rows FROM deb.user_purchase;",

),

with DAG(
    dag_id = DAG_ID, 
    start_date=days_ago(1),
    schedule_interval="@once",
    catchup=False
) as dag:
    start_workflow = DummyOperator(task_id="start_worklow")
    validate = DummyOperator(task_id="validate")
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="pg_db", 
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
    get_table_count = PostgresOperator(
        task_id="get_table_count",
        postgres_conn_id="pg_db",
        sql="""SELECT COUNT(*) AS total_rows FROM deb.user_purchase;""",

    ),
    load = DummyOperator(
        task_id="load",
        postgres_conn_id="pg_db", 
    )
    end_workflow = DummyOperator(task_id="end_worklow")

    start_workflow >> validate >> prepare >> get_table_count >> load >> end_workflow