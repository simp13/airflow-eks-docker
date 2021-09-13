from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy',value=accuracy)

def _choose_best_model(ti):
    print('choose best model')
    fetched_accuracies = ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A','training_model_B','training_model_C'])
    for fetched_accuracy in fetched_accuracies:
        if fetched_accuracy > 5:
            return 'accurate'
        return 'inaccurate'

with DAG('condition_tasks',schedule_interval='@daily',default_args=default_args,catchup=False) as dag:

    downloading_files = BashOperator(
        task_id = 'downloading_files',
        bash_command = 'sleep 3',
        do_xcom_push = False
    )

    training_model_task = [
        PythonOperator(
            task_id=f'training_model_{task}',
            python_callable=_training_model
            ) for task in ['A', 'B', 'C']
    ]

    choose_model = BranchPythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )
    
    accurate = DummyOperator(
        task_id = 'accurate'
    )

    inaccurate = DummyOperator(
        task_id = 'inaccurate'
    )

    store = BashOperator(
        task_id = 'store',
        bash_command = 'echo {{ var.value.Test }}',
        trigger_rule = 'none_failed_or_skipped'
    )


    downloading_files >> training_model_task >> choose_model

    choose_model >> [accurate,inaccurate] >> store




    

