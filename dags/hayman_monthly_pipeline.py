from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _get_token_from_haymen_backend_api():
    print("Token -> testing")

def _get_list_and_update_office(**kwargs):
    office_id = kwargs['officeid']
    print("Office id -> ",office_id)

with DAG('hayman_monthly_data_pipeline',schedule_interval='@monthly',default_args=default_args,catchup=False) as dag:

    office_ids = [1,2,3,4,5,10]

    get_token_from_haymen_backend_api = PythonOperator(
        task_id='get_token_from_haymen_backend_api',
        python_callable=_get_token_from_haymen_backend_api
    )

    update_data_tasks = [
        PythonOperator(
            task_id=f'get_list_and_update_office_{office_id}',
            python_callable=_get_list_and_update_office,
            op_kwargs={'officeid': office_id}
        )
        for office_id in office_ids]

    # parallel
    # get_token_from_haymen_backend_api >> update_data_tasks

    # sequential
    tasks = [get_token_from_haymen_backend_api] + update_data_tasks
    for i in range(len(tasks)):
        if i not in [0]:
            tasks[i-1] >> tasks[i]
