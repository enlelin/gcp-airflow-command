from airflow import DAG
import os, datetime, logging, time
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

yesterday = datetime.datetime.combine(
   datetime.datetime.today() - datetime.timedelta(1),
   datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

    
bq_dag = DAG('bq-drive', default_args=default_dag_args, schedule_interval=datetime.timedelta(days=1), catchup=False)

# Make sure to create the connection first
bq_op_drive = BigQueryOperator(
    task_id='bigquery_task_id',
    bql='SELECT * FROM `my_test.my_table` LIMIT 1000',
    bigquery_conn_id='my_connections',
    use_legacy_sql= False,
    dag=bq_dag)

bq_op_drive
