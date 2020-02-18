import datetime
from airflow.models import DAG
from airflow.operators import python_operator

default_dag_args = {
    'start_date' : datetime.datetime(2018,1,1),
    'schedule_interval': datetime.timedelta(days=1),
}

with DAG (
    'my_first_dag',
    default_args = default_dag_args) as dag:
    
    def sum_square(a,b):
        return (a+b)*(a+b)
    
    def do_math():
        import logging
        logging.info("Here is the result")
        logging.info(sum_square(1,2))
    
    do_some_math = python_operator.PythonOperator(task_id="calculate", python_callable=do_math)
    
    do_some_math
