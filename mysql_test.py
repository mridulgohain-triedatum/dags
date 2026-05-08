from airflow import DAG
from airflow import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz="America/Chicago"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sql_query_op_test_mysql_23_10',
    default_args=default_args,
    description='Converted from Autosys JIL',
    schedule='10 1 * * *',
    catchup=False,
    tags=['autosys-conversion'],
)



with TaskGroup(
    group_id='box_db_mysql',
    tooltip='"Box to hold file watcher and command job"',
    dag=dag,
) as box_db_mysql:

    insert_into_table = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='mysql_localhost',
        sql="insert into test values (4,'xyz')",
        split_statements=True,
        return_last=False,
        dag=dag,
    )


    process_sp1 = SQLExecuteQueryOperator(
        task_id='process_sp1',
        conn_id='mysql_localhost',

        sql=f"CALL test_sp(%(f_name)s, %(l_name)s, %(age)s)",
        parameters={'f_name': 'Mridul', 'l_name': 'Gohain', 'age': 133},

        dag=dag,
    )

    insert_into_table >> process_sp1
