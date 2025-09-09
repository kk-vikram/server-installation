from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="*/2 * * * *",   # ✅ every 2 minutes
    catchup=False
) as dag:

    task = BashOperator(
        task_id="print_hello",
        bash_command='echo "Hello, World!"'
    )
