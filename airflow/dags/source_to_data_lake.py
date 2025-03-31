from utils.etl_functions import web_to_gcs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

with DAG(
    dag_id="source_to_data_lake",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={
         "year": Param(2019, type="integer", minimum=2019, maximum=2020),
         "service": Param("yellow", type="integer")
    }
) as dag:

    task = PythonOperator(
        task_id="say_hello",
        python_callable=web_to_gcs,
        op_kwargs={
            ""
        }
    )

    task