from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# -----------------------
# DAG DEFINITION
# -----------------------
dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "ingestion"],
)

# -----------------------
# TASK 1: CREATE TABLE
# -----------------------
def create_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            joindate DATE
        );
    """)

# -----------------------
# TASK 2: TRUNCATE TABLE
# -----------------------
def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres")
    hook.run("TRUNCATE TABLE raw_employee_data;")

# -----------------------
# TASK 3: LOAD CSV
# -----------------------
def load_csv_data():
    file_path = "/opt/airflow/data/input.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found at {file_path}")

    df = pd.read_csv(file_path)

    if df.empty:
        raise ValueError("CSV file is empty")

    hook = PostgresHook(postgres_conn_id="postgres")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        name="raw_employee_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    return f"Loaded {len(df)} rows into raw_employee_data"

# -----------------------
# TASK DEFINITIONS
# -----------------------
create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_data,
    dag=dag,
)

# -----------------------
# DEPENDENCIES
# -----------------------
create_table_task >> truncate_table_task >> load_csv_task
