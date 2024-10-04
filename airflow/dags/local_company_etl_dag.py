# Playwright is not being installed correctly in the container so the DAG is Broken. 
# from datetime import datetime, date
# from airflow import DAG
# from airflow.models.variable import Variable
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from local_company_lookup.utils import collect_local_companies

# # Variable here is a global storage for airflow variables that need to be accessed by multiple DAGs
# BASE_DATA_COLLECTION_PATH = Variable.get("BASE_DATA_COLLECTION_PATH") or "/opt/airflow/data_collection/"
# BASE_DATA_LAKE_PATH = Variable.get("BASE_DATA_LAKE_PATH") or "/opt/airflow/data_lake/"

# default_args = {
#     'start_date': datetime(2024, 1, 1),
#     'catchup': False,
#     'is_paused_upon_creation': True,
# }

# # Book collection and refinement ETL
# with DAG(
#     dag_id="local_company_etl_dag", 
#     default_args=default_args,
#     schedule_interval="@monthly",
#     render_template_as_native_obj=True,
# ) as book_dag:
#     dummy_task_books = DummyOperator(task_id="dummy_task_books")

#     collect_local_companies_task = PythonOperator(
#         task_id="collect_local_companies",
#         python_callable=collect_local_companies,
#     )

#     dummy_task_books >> collect_local_companies_task
