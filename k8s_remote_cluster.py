from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="k8s_remote_cluster",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    run_container = KubernetesPodOperator(
        task_id="run_hello_python",
        name="hello-python-task",
        namespace="zk29hbf",
        image="mridulgohain/hello-python:v2",
        image_pull_policy="Always",
        kubernetes_conn_id="k8s_conn",
        is_delete_operator_pod=True,  # auto delete pod (like TTL)
        get_logs=True,
    )

    run_container
