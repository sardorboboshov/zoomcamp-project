from utils.etl_functions import web_to_gcs, list_of_files, download_files
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

DATASET_PATH = Variable.get('DATASET_PATH')

BUCKET_NAME = Variable.get('BUCKET_NAME')
DESTINATION_PATH = Variable.get('DATA_LAKE_DESTINATION_PATH')  # Path inside GCS bucket

with DAG(
    dag_id="source_to_data_lake",
    start_date=datetime(2025, 3, 15),
    schedule_interval=None,
    catchup=True,
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

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=DATASET_PATH + '/*',  # Local file path
        dst=DESTINATION_PATH,  # Destination path in GCS
        bucket=BUCKET_NAME,  
        gcp_conn_id="google_cloud",  # Matches Airflow connection ID
    )

    task1 >> task2 >> upload_file