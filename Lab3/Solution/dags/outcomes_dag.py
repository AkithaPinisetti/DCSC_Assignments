import os
import sys
import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from etlscripts.TransformData import transform_data
from etlscripts.APItoGCP import main
from etlscripts.LoadData import load_data_to_postgres

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5)
}

with DAG(
    dag_id="outcomes_dag",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
        start = BashOperator(task_id = "START",
                             bash_command = "echo start")

        extract_data_from_api_to_gcp =  PythonOperator(task_id = "API_TO_GCP",
                                                  python_callable = main,)

        transform_data_from_gcp_step = PythonOperator(task_id="GCP_TRANSFORMED_DATA",
                                              python_callable=transform_data,)

        load_dim_animals_tab = PythonOperator(task_id="LOAD_DIMENSION_ANIMALS",
                                            python_callable=load_data_to_postgres,
                                             op_kwargs={"file_name": 'dim_animal.csv', "table_name": 'animaldimension'},)

        load_dim_outcome_types_tab = PythonOperator(task_id="LOAD_DIMENSION_OUTCOME_TYPES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_outcome_types.csv', "table_name": 'outcomedimension'},)
        
        load_dim_dates_tab = PythonOperator(task_id="LOAD_DIMENSION_DATES",
                                             python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'dim_dates.csv', "table_name": 'datedimension'},)
        
        load_fct_outcomes_tab = PythonOperator(task_id="LOAD_FACT_OUTCOMES",
                                              python_callable=load_data_to_postgres,
                                              op_kwargs={"file_name": 'fct_outcomes.csv', "table_name": 'outcomesfact'},)
        
        end = BashOperator(task_id = "END", bash_command = "echo end")

        start >> extract_data_from_api_to_gcp >> transform_data_from_gcp_step >> [load_dim_animals_tab, load_dim_outcome_types_tab, load_dim_dates_tab] >> load_fct_outcomes_tab >> end