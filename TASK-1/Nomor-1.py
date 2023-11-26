#Input package
from datetime import datetime
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator

#function untuk return String
def print_hello():
    return 'Hello world from first Airflow DAG! My name Elsa'

#Define DAG 
dag = DAG(
        'alterra_hello_world-elsa', # nama dag
        description='Hello World DAG',
        schedule_interval='0 */5 * * *', #Berjalan setiap 5jam
        start_date=datetime(2023, 10, 30),  #Kapan akan dijalankan
        catchup=False 
    )

#Define task
operator_hello_world = PythonOperator( #nama task & operator
    task_id='hello_task', 
    python_callable=print_hello, 
    dag=dag 
)

operator_hello_world #define task untuk running