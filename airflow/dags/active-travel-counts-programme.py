from utils.etl_functions import list_of_files, download_files, process_active_travel_counts
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import polars as pl

DATASET_PATH = Variable.get('DATASET_PATH')

BUCKET_NAME = Variable.get('BUCKET_NAME')
DESTINATION_PATH = Variable.get('DATA_LAKE_DESTINATION_PATH')  # Path inside GCS bucket
PROGRAM = "ActiveTravelCountsProgramme"
FILE_NAME = 'ActiveTravelCountsProgramme'
with DAG(
    dag_id='ActiveTravelCountsProgramme',
    start_date=datetime(2025, 3, 15),
    schedule_interval=None,
    catchup=True,
) as dag:

    task1 = PythonOperator(
        task_id="fetch_file_urls",
        python_callable=list_of_files,
        op_kwargs={
            'program': PROGRAM
        }
    )

    task2 = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        provide_context=True,
        op_kwargs={
            'dataset_path':DATASET_PATH + f'/{PROGRAM}'
        }
    )
    
    
    process_files = PythonOperator(
        task_id="process_files",
        python_callable=process_active_travel_counts,
        op_kwargs={
            'dataset_path': DATASET_PATH + f'/{PROGRAM}',
            'file_name': FILE_NAME
        }
    )
    
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=DATASET_PATH + f'/{PROGRAM}/{FILE_NAME}.parquet',  # Local file path
        dst=DESTINATION_PATH + f'/{PROGRAM}/{FILE_NAME}.parquet',  # Destination path in GCS
        bucket=BUCKET_NAME,  
        gcp_conn_id="google_cloud",  # Matches Airflow connection ID
    )

    

    task1 >> task2 >> process_files >> upload_file