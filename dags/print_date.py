from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    "owner": "vikram",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id="print_date_every_1_min",
    default_args=default_args,
    description="A simple DAG that prints the date every 1 minute",
    schedule="* * * * *",   # every 1 minute
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Task: print date
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    print_date
