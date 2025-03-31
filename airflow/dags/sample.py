from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a simple function to print a message
def hello_world():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id="sample_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world,
    )

    task
