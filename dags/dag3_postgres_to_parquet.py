from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os
import pyarrow.parquet as pq

dag = DAG(
    dag_id='postgres_to_parquet_export',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['export', 'parquet'],
)

def check_table_exists(table_name: str):
    hook = PostgresHook(postgres_conn_id='postgres')
    # Check existence
    exists = hook.get_first("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{}');".format(table_name))
    if not exists[0]:
        raise ValueError(f"Table {table_name} does not exist")
    # Check row count >0
    row_count = hook.get_first(f"SELECT COUNT(*) FROM {table_name}")[0]
    if row_count == 0:
        raise ValueError(f"Table {table_name} has no data (rows: {row_count})")

def export_table_to_parquet(table_name: str, output_path: str):
    hook = PostgresHook(postgres_conn_id='postgres')
    df = hook.get_pandas_df(f"SELECT * FROM {table_name}")
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )
    stat = os.stat(output_path)
    return {
        'file_path': output_path,
        'row_count': len(df),
        'file_size_bytes': stat.st_size
    }

def validate_parquet(file_path: str):
    df = pd.read_parquet(file_path)
    expected_cols = ['id', 'name', 'age', 'city', 'salary', 'joindate', 'fullinfo', 'agegroup', 'salarycategory', 'yearjoined']
    if not all(col in df.columns for col in expected_cols):
        raise ValueError(f"Missing columns: {set(expected_cols) - set(df.columns)}")
    if len(df) == 0:
        raise ValueError("Parquet file is empty")
    # Additional schema checks (dtypes approximate)
    if df['id'].dtype != 'int64' or 'fullinfo' not in df.columns:
        raise ValueError("Schema mismatch")

check_table_task = PythonOperator(
    task_id='check_source_table_exists',
    python_callable=check_table_exists,
    op_kwargs={'table_name': 'transformed_employee_data'},
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_parquet',
    python_callable=export_table_to_parquet,
    op_kwargs={
        'table_name': 'transformed_employee_data',
        'output_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'
    },
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_parquet_file',
    python_callable=validate_parquet,
    op_kwargs={'file_path': '/opt/airflow/output/employee_data_{{ ds }}.parquet'},
    dag=dag
)

check_table_task >> export_task >> validate_task
