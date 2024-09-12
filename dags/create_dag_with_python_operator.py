from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

dag_owner = 'tupchiienko'

default_args = {
        'owner': dag_owner,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
        }

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello world my name is {first_name} {last_name},"
          f"and im {age} yers old") 
    

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')   

def get_age(ti):
    ti.xcom_push(key='age', value='35')     

with DAG(dag_id='our_dag_with_python_operator_v7',
        default_args=default_args,
        description='first dag with python operator',
        start_date=datetime(2024, 9, 11),
        schedule_interval='@daily'
) as dag:    
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={ 'age':20}
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )
    
    [task3,task2] >> task1
