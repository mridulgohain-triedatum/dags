from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def wait_10_seconds(task_name):
    print(f"Starting {task_name}")
    time.sleep(10)
    print(f"Finished {task_name}")

with DAG(
    dag_id="conn_test_mssql",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mssql"],
) as dag:
    py_task = PythonOperator(
        task_id="task_1",
        python_callable=wait_10_seconds,
        op_args=["task_1"],
    )

    # 2. Swapped MsSqlOperator -> SQLExecuteQueryOperator
    view_databases = SQLExecuteQueryOperator(
        task_id="view_databases",
        conn_id="mssql_localhost",
        sql="""
            SELECT name FROM sys.databases;
        """,
        show_return_value_in_logs=True,
    )
    py_task >> view_databases
