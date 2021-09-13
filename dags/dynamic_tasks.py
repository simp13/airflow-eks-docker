from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from random import randint
default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _generate_account_ids(ti=None):
    account_ids = list(range(randint(1,10)))
    print(account_ids)
    if ti is not None:
        ti.xcom_push(key='account_ids',value=account_ids)
    return account_ids

def _crawl_account():
    print("Hello World")

def _generate_crawl_tasks(number,**kwargs):
    print("Account number",number)
    return PythonOperator(
        task_id=f'crawl_accountnumber_{number}',
        python_callable=_crawl_account,
    )



with DAG('dynamic_tasks',schedule_interval='*/1 * * * *',default_args=default_args,catchup=False) as dag:

    generate_account_ids = PythonOperator(
        task_id='generate_account_ids',
        python_callable=_generate_account_ids,
    )
    
    # parallel
    # for i in _generate_account_ids():
    #     generate_account_ids >> _generate_crawl_tasks(i)

    # sequential
    a = [generate_account_ids]
    for i in range(len(_generate_account_ids()) + 1):
        a.append(PythonOperator(
            task_id=f'crawl_accountnumber_{i}',
            python_callable=_crawl_account,
            trigger_rule="all_done"
        ))

        if i not in [0]:
            a[i - 1] >> a[i]