from utils.etl_functions import web_to_gcs, list_of_files, download_files
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime

with DAG(
    dag_id="source_to_data_lake",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="fetch_file_urls",
        python_callable=list_of_files,
    )

    task2 = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        provide_context=True
    )

    task1 >> task2