from datetime import datetime
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.python import PythonOperator

def read_variable_callable():
    var_value = Variable.get("var_1", default="Variable not found!")
    print(f"--- The value of 'var_1' is: {var_value} ---")


def update_variable_callable():
    new_value = f"Updated_at_{datetime.now().isoformat()}"

    Variable.set(key="var_2", value=new_value)
    print(f"--- Successfully updated 'var_2' to: {new_value} ---")

with DAG(
    dag_id="recurring_variable_test",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["variables", "airflow3"],
) as dag:
    read_task = PythonOperator(
        task_id="read_var",
        python_callable=read_variable_callable,
    )
    update_task = PythonOperator(
        task_id="update_var",
        python_callable=update_variable_callable,
    )
    read_task >> update_task
