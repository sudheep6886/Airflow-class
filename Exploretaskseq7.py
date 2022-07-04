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
      return 'xaccurate'
   return 'inaccurate'



with DAG(dag_id='branching1', schedule_interval='@daily', default_args=default_args, catchup=False,tags=['JP','Learning','Batch']) as dag:
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
	 
choose_best_model >> [accurate, inaccurate]
