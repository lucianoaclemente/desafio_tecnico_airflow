# desafio_tecnico_airflow

Job para encontrar carrinhos abandonados de clientes.<br />
Desafio executado em uma máquina virtual Ubuntu 20.04, Python 3.8, AirFlow 2.0.1.<br />

Instruções<br />

* Copiar o arquivo **desafio_tecnico.py** para **$AIRFLOW_HOME/dags**
* Criar a pasta **$AIRFLOW_HOME/dags/input** e copiar o arquivo **page-views.json**
* Criar a pasta **$AIRFLOW_HOME/dags/output**

Para executar, digite **airflow dags trigger desafio_tecnico** ou pelo localhost:8080, executando **airflow webserver** e **airflow scheduler**. Acesse o gerenciador e inicie a DAG através da action **Trigger DAG**.

