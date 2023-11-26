from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime 
import os

from etlscripts.transform import transform_data

SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
PQ_TARGET_DIR = AIRFLOW_HOME+'/data/{{ ds }}/processed'

with DAG(
    dag_id="outcomes_dag",
    start_date= datetime(2023,11,23),
    schedule_interval = '@daily'

) as dag: 

    step1 = BashOperator(
        task_id="step1",
        bash_command = "cd /opt/airflow/data/2023-11-23/downloads",
        bash_command = "ls -ld"
        bash_command
        # bash_command=f"curl -create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    )

    step2 = PythonOperator(
        task_id="transform",
        python_callable = transform_data,
        op_kwargs = {
            'source_csv' : CSV_TARGET_FILE,
            'target+dir' : PQ_TARGET_DIR
        }
    )

    step1 >> step2
