import logging
from datetime import datetime, date
from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models.param import Param


# Variable here is a global storage for airflow variables that need to be accessed by multiple DAGs
BASE_DATA_COLLECTION_PATH = Variable.get("BASE_DATA_COLLECTION_PATH") or "/opt/airflow/data_collection/"
BASE_DATA_LAKE_PATH = Variable.get("BASE_DATA_LAKE_PATH") or "/opt/airflow/data_lake/"

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    'is_paused_upon_creation': True,
}

# Book collection and refinement ETL
with DAG(
    dag_id="book_etl_dag", 
    default_args=default_args,
    schedule_interval="@monthly",
    params={
        "open_library_ids": Param([], type="array", items={"type": "string"}),
    },
    render_template_as_native_obj=True,
) as book_dag:
    dummy_task_books = DummyOperator(task_id="dummy_task_books")

    collect_books_task = BashOperator(
        task_id="collect_raw_books",
        bash_command=f"python3 {BASE_DATA_COLLECTION_PATH}collect_books.py --data_lake_path $DATA_LAKE_PATH --open_library_ids $OPEN_LIBRARY_IDS --execution_date $EXECUTION_DATE",
        env={
            "DATA_LAKE_PATH": BASE_DATA_LAKE_PATH,
            "OPEN_LIBRARY_IDS": "{{params.open_library_ids | join(',')}}",
            "EXECUTION_DATE": "{{logical_date | ds}}"
        },
    )

    refine_books_task = BashOperator(
        task_id="refine_books",
        bash_command=f"python3 {BASE_DATA_COLLECTION_PATH}treat_books.py --data_lake_path $DATA_LAKE_PATH",
        env={
            "DATA_LAKE_PATH": BASE_DATA_LAKE_PATH,
        }
    )

    dummy_task_books >> collect_books_task >> refine_books_task
