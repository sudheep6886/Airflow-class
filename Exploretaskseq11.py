from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
'start_date': datetime(2020, 1, 1)
}
def _choose_best_model():
   accuracy = 6
   EOM=True
   if accuracy > 5:
      if EOM==True:
        return ['accurate','billing']
      else:
         return 'accurate'
   else:
      return 'inaccurate'

with DAG(dag_id='branching4_EOM',
   schedule_interval='@daily',
   default_args=default_args,
   catchup=False,
   tags=['JP','Learning','Batch','Sudheep']
) as dag:
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
  billing = DummyOperator(
   task_id='billing'
  )
  final_task = DummyOperator(
   task_id='final_task',
   trigger_rule='none_failed_or_skipped'
  )
choose_best_model >> [accurate, inaccurate, billing] >> final_task

