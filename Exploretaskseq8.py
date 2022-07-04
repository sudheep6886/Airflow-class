from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
'start_date': datetime(2020, 1, 1)
}

def _choose_best_model():
   accuracy = 6
   if accuracy > 5:
      return 'accurate'
   return 'inaccurate'



with DAG(dag_id='branching2', schedule_interval='@daily', default_args=default_args, catchup=False,tags=['JP','Learning','Batch']) as dag:
     choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model
     )
	 
     accurate = DummyOperator(
        task_id='accurate'
     )
	 
     inaccurate = DummyOperator(
         task_id='inaccurate'
     )
	 
     apprun = DummyOperator(
         task_id='apprun',
         trigger_rule='none_failed_or_skipped'
     )
choose_best_model >> [accurate, inaccurate] >> apprun
