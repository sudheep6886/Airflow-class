#IMPORTS

import pendulum
from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

#BATCH RUN CONFIGURATION
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2022, 1, 1, tz="Asia/Singapore"),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#DEFINITION OF THE DAG
with DAG(dag_id='Explore_task_sequence2',
        default_args=default_args,
        schedule_interval='@once',
        dagrun_timeout=timedelta(seconds=120),
        catchup=False,
        tags=['JP','Learning','Batch']
        ) as dag:

#TASKS DEFINITION
   t1 = BashOperator(
      task_id='task1',
      bash_command="echo 'Task 1'")

   t2 = BashOperator(
      task_id='task2',
      bash_command="echo 'Task 2'")

   t3 = BashOperator(
      task_id='task3',
      bash_command="echo 'Task 3'")

   t4 = BashOperator(
      task_id='task4',
      bash_command="echo 'Task 4'")

   t5 = BashOperator(
      task_id='task5',
      bash_command="echo 'Task 5'")

   t6 = BashOperator(
      task_id='task6',
      bash_command="echo 'Task 6'")

#TASK SEQUENCE
t1>>[t2,t3]>>t4>>t5>>t6
