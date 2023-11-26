from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime 

with DAG(
    dag_id="outcomes_dag",
    start_date= datetime(2023,11,1)
) as dag: 

    step1 = BashOperator(
        task_id="step1",
        bash_command="echo 1",
    )

    step2 = BashOperator(
        task_id="step2",
        bash_command="echo 2",
    )

    step1 >> step2
