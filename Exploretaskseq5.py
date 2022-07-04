from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='example_5',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['JP','Learning','Batch'],
) as dag:
    # [START howto_operator_short_circuit]
    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: False,
    )
    task_true=EmptyOperator(task_id="task_true")
    task_false=EmptyOperator(task_id="task_false")
    ds_true = [task_true]
    ds_false = [task_false]

    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)
    # [END howto_operator_short_circuit]
       
   
    task_1=EmptyOperator(task_id="task_1")
    # task_2=EmptyOperator(task_id="task_2")
   
    # task_7 = EmptyOperator(task_id="task_7", trigger_rule=TriggerRule.ALL_DONE)
    task_7 = EmptyOperator(task_id="task_7")

    short_circuit = ShortCircuitOperator(
        task_id="short_circuit", ignore_downstream_trigger_rules=True, python_callable=lambda: True
    )

    task_1>>short_circuit>>task_7
    # chain(task_1,short_circuit, task_7)
    # [END howto_operator_short_circuit_trigger_rules]
