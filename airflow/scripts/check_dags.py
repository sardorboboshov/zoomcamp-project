# scripts/check_dags.py
import os
import sys
from airflow.models import DagBag


def main():

    dag_folder = os.getenv('DAG_FOLDER', 'dags')
    dag_bag = DagBag(dag_folder, include_examples=False)

    if dag_bag.import_errors:
        print("Errors while importing DAGs!")
        for dag_id, error in dag_bag.import_errors.items():
            print(f"{dag_id}: {error}")
        sys.exit(1)
    else:
        print("All DAGs parsed successfully.")


if __name__ == "__main__":
    main()
