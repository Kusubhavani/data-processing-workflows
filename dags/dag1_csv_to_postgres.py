from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

dag = DAG(
    dag_id='csv_to_postgres_ingestion',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'ingestion'],
)

def create_employee_table():
    hook = PostgresHook(postgres_conn_id='postgres')  # Default conn_id='postgres' matches docker-compose env
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            joindate DATE
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_employee_data;")
    conn.commit()
    cur.close()
    conn.close()

def load_csv_data():
    df = pd.read_csv('/opt/airflow/data/input.csv')
    hook = PostgresHook(postgres_conn_id='postgres')
    df.to_sql(
        name='raw_employee_data',
        con=hook.get_sqlalchemy_engine(),
        if_exists='append',  # Append after truncate
        index=False,
        method='multi',  # Chunked insert for efficiency
        chunksize=1000
    )
    return len(df)

create_table_task = PythonOperator(
    task_id='create_table_if_not_exists',
    python_callable=create_employee_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id='truncate_table',
    python_callable=truncate_employee_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_data,
    dag=dag,
)

create_table_task >> truncate_table_task >> load_csv_task
