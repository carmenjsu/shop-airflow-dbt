from airflow import DAG
from airflow.operation.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'email': 'carmenjssu@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='order_monitor_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='source ""
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "Order monitoring completed."',
    )

    start >> end