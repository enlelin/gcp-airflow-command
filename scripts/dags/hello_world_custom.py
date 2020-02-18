from custom_operator import HelloOperator
from datetime import datetime
from airflow import DAG

dag = DAG('hello_world_custom',
         description='Simple tutorial DAG',
         schedule_interval='0 12 * * *',
         start_date=datetime(2020, 1, 21),
         catchup=False)

hello_task = HelloOperator(task_id='sample_task', name='Enle Lin', dag=dag)

hello_task
