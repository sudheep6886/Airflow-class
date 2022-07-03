
#To run in aitflow copy this file to the dags directory

import pendulum
from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #when the batch should have started,it can be set in the future.
    'start_date': pendulum.datetime(2022, 1, 1, tz="Asia/Singapore"),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(dag_id='batchwith2tags',
        default_args=default_args,
        #When the dag is scheduled in cron format
        schedule_interval='*/5 * * * *',
        dagrun_timeout=timedelta(seconds=120),
        #this option is to not rerun the previous or missed executions.
        catchup=False,
        # tags for easy tracking of group of tasks
        tags=['JP','Learning','Batch']
        ) as dag:
# definition of task 1
   t1 = BashOperator(
      task_id='task_1',
      bash_command="echo 'Task 1 - Hello Jean-Paul '")

# definition of task 2
   t2 = BashOperator(
      task_id='task_2',
      bash_command="echo 'Task 2 - Goodbye!'")
#Actual dependencies definition.
t1 >> t2
