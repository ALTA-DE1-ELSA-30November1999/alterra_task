from airflow import DAG
from datetime import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'alterra_hook_elsa',
    schedule=None,
    start_date=datetime(2022, 10, 21),
    catchup=False
) as dag:

    create_table_in_db_task = PostgresOperator(
        task_id = 'create_table_in_db',
        sql = ('CREATE TABLE IF NOT EXISTS gender_name_prediction ' +
        '(' +
            'input TEXT, ' +
            'details TEXT, ' +
            'result_found BOOLEAN, ' +
            'first_name TEXT, ' +
            'probability FLOAT(53), ' +
            'gender TEXT, ' +
            'timestamp TIMESTAMP ' +
        ')'),
        postgres_conn_id='pg_conn_id', 
        autocommit=True,
        dag=dag
    )

    def loadDataToPostgres():
        pg_hook = PostgresHook(postgres_conn_id='pg_conn_id').get_conn()
        curr = pg_hook.cursor("cursor")
        with open('/opt/airflow/dags/tugas.json', 'r') as file:
            next(file)
            curr.copy_from(file, 'gender_name_prediction', sep='\t')
            pg_hook.commit()


    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=loadDataToPostgres,
        dag=dag
    )


    create_table_in_db_task >> load_data_to_db_task