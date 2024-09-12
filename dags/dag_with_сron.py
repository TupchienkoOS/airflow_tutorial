from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 

dag_owner = 'tupchiienko'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dag_with_cron_expr_v04',
        default_args=default_args,
        description='',
        start_date=datetime(2024, 9, 10),
        schedule_interval='0 3 * * Tue-Thu'
) as dag:    
    task1 = BashOperator(
        task_id="task1",
        bash_command='echo "Hi from bash operator"'
    )
    task1