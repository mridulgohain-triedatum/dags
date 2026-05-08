from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_message():
    print("Hello from Airflow!")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="simple_hello_world_dag",
    default_args=default_args,
    description="A simple Airflow DAG example",
    start_date=datetime(2026, 5, 1),
    schedule="*/5 * * * *",  # Runs every 5 minutes
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_message,
    )

    hello_task
