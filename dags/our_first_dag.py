from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'code2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='our_first_dag_v5',
    default_args=default_args,
    description='This our first dag that we write',
    start_date=datetime(2024, 9, 10),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is first task"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo hello world, this is second task"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo third task i will running after task 1"
    )


    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3]