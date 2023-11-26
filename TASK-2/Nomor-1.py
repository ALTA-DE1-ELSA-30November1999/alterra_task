from airflow import DAG
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id = 'alterra_connection_elsa',
    schedule=None,
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:
    
    get_statistic = SimpleHttpOperator(
        task_id="get_statistic",
        endpoint="/statistic",
        method="GET",
        http_conn_id="gender_api", # koneksi ke airflow
        log_response=True,
        dag=dag
    )

    identify_name = SimpleHttpOperator(
        task_id="post_name",
        endpoint="/country-of-origin", # merujuk pada API Documentation
        method="POST",
        data='{"full_name":"sophia5342@gmail.com"}', # merujuk pada API Documentation
        http_conn_id="gender_api",
        log_response=True,
        dag=dag
    )

    def my_uri():
        from airflow.hooks.base import BaseHook
        print(f"Gender API URI ", BaseHook.get_connection("gender_api").get_uri())

    print_uri = PythonOperator(
        task_id = "print_uri",
        python_callable = my_uri
    )
    
    get_statistic >> identify_name >> print_uri