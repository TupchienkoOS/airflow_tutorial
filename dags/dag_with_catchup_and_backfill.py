from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 

dag_owner = 'otupchiienko'

default_args = {'owner': dag_owner,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(
        dag_id='dag_with_catchup_and_backfill_v0Z',
        default_args=default_args,
        description='',
        start_date=datetime(2023, 9, 11),
        schedule_interval='@daily',
        catchup=False
) as dag: 
     task1 = BashOperator(
        task_id="task1",
        bash_command='echo Hi from bash operator'
    )