
from airflow.decorators import dag, task
from datetime import datetime, timedelta 

dag_owner = 'tupchiienko'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

@dag(dag_id='dag_with_taskflow_api_v02',
     default_args=default_args,
     start_date=datetime(2024, 9, 11),
     schedule_interval='@daily')
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
             'first_name':'Jerry',
             'last_name': 'Fridman'
        }
    
    @task()
    def get_age():
        return 35
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hello world my name is {first_name} {last_name},"
          f"and im {age} yers old") 
        
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], last_name=name_dict['last_name'], age=age)

greet_dag = hello_world_etl()



    