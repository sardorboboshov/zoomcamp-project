from utils.etl_functions import list_of_files, download_files,process_usage_stats
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

DATASET_PATH = Variable.get('DATASET_PATH')

BUCKET_NAME = Variable.get('BUCKET_NAME')
DESTINATION_PATH = Variable.get('DATA_LAKE_DESTINATION_PATH')  # Path inside GCS bucket
PROGRAM='usage-stats'
FILE_NAME = 'usage-stats'
with DAG(
    dag_id="usage-stats",
    start_date=datetime(2025, 3, 15),
    schedule='@monthly',
    catchup=True,
    tags=['etl']
) as dag:

    fetch_file_urls_task = PythonOperator(
        task_id="fetch_file_urls",
        python_callable=list_of_files,
        op_kwargs={
            'program': PROGRAM
        }
    )

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        op_kwargs={
            'dataset_path':DATASET_PATH + f'/{PROGRAM}'
        }
    )
    
    # info_columns = PythonOperator(
    #     task_id="info_columns",
    #     python_callable=dev_info_usage_stats_cols,
    #     op_kwargs={
    #         'dataset_path': DATASET_PATH + f'/{PROGRAM}'
    #     }
    # )
    process_files = PythonOperator(
        task_id="process_files",
        python_callable=process_usage_stats,
        op_kwargs={
            'dataset_path': DATASET_PATH + f'/{PROGRAM}',
            'file_name': FILE_NAME
        }
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=DATASET_PATH + f'/{PROGRAM}/*.parquet',  # Local file path
        dst=DESTINATION_PATH + f'/{PROGRAM}/',  # Destination path in GCS
        bucket=BUCKET_NAME,  
        gcp_conn_id="google_cloud",  # Matches Airflow connection ID
    )

    

    (
        fetch_file_urls_task 
        >> download_files_task 
        # >> info_columns 
        >> process_files 
        >> upload_file
    )