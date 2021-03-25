#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
