from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

'''def ingest_data():
    hook = PostgresHook(postgres_conn_id)
    hook.insert_rows(
        table="deb.user_purchase",
        rows=[
            [
                "Jan 2000",
                1,
                "The Weeknd",
                "Out of time",
                100.01,
                1,
                2,
                3,
                4,
                5,
                6,
            ]
        ]
    )'''

with DAG(
    dag_id = "db_pg_ingestion", 
    start_date=days_ago(1)
) as dag:
    start_workflow = DummyOperator(task_id="start_worklow")
    validate = DummyOperator(task_id="validate")
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_worklow")

    start_workflow >> validate >> load >> end_workflow