from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
#from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

dag_owner = 'tupchiienko'

default_args = {'owner': dag_owner,
        'retries': 0
        }

with DAG(dag_id='account_type_dag',
        default_args=default_args,
        description='test dag for acoount_type ptg and tgt layer',
        start_date=datetime(2024, 9 , 12),
        schedule_interval='0 * * * *',
        catchup=False
) as dag:
     task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is first task"
    )
     task2 = BashOperator(
         task_id="bash_task",
         bash_command='echo "Hi from bash operator"'
     )
     task1 >> task2
   