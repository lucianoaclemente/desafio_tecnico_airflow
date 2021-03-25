"""Used for unit tests"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import json

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='desafio_tecnico', 
    schedule_interval=None,     
    tags=['desafio'])

def read_transactions(ds, **kwargs):

    abandoned_list = []

    with open('/home/desenv/airflow/dags/input/page-views.json') as json_file:
        
        current_customer = ""
        last_transaction = {}
        
        transaction_list = json.load(json_file)    

        for t in transaction_list:
            if t['customer'] != current_customer:
                current_customer = t['customer']
                
                # verifica se a transacao foi abandonada
                if 'page' in last_transaction:
                    if last_transaction['page'] != "checkout":
                        log = { "timestamp": last_transaction['timestamp'] , "customer": last_transaction['customer'], "product": last_transaction['product'] }
                        abandoned_list.append(log)
                    
            last_transaction = t

        with open('/home/desenv/airflow/dags/output/abandoned-carts.json', 'w') as outfile:
            json.dump(abandoned_list, outfile)


task_read_transactions = PythonOperator(
    task_id='read_transactions',
    python_callable=read_transactions,
    dag=dag, 
    start_date=days_ago(2),  
    )

task_read_transactions 
