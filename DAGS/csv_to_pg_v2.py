from asyncio import tasks
import logging
from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

DAG_ID = "csv_to_pg_v2"

def get_table_count():
    pg_hook = PostgresHook(postgres_conn_id='pg_conn')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute("SELECT COUNT(*) AS total_rows FROM deb.user_purchase")
    #cursor.close()
    #pg_conn.close
    logging.info("returning count: ")
    cursor.fetchone()


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
    load = PostgresOperator(
        task_id="load",
        postgres_conn_id="pg_conn", 
        sql="""
            \copy deb.user_purchase(InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country)
                FROM 'C:\\Users\\mgarc\\Documents\\wizeline\\bootcamp\\data-engineering-bootcamp\\DATA\\user_purchase.csv'
                DELIMITER ','
                CSV HEADER;
            SELECT COUNT(*) AS total_rows FROM deb.user_purchase;
            """
    )
    end_workflow = DummyOperator(task_id="end_worklow")

    start_workflow >>  create_table >> count >> load >> end_workflow