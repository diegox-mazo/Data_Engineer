from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from juegos_etl import main
from modules.email_service import send_email

default_args = {
    'owner': 'DiegoMazo',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id='DiegoMazo_Proyecto_Final',
    description='Obtiene los mas recientes juegos en oferta de Steam',
    start_date=datetime(2024, 5, 19, 12),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task_1 = PythonOperator(
        task_id='ingestion_data',
        python_callable=main
    )

    task_2 = PythonOperator(
        task_id='send_mail',
        python_callable= send_email
    )

    task_1 >> task_2