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
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
)

# -----------------------
# TASK 1: CHECK SOURCE TABLE
# -----------------------
def check_table_exists():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        "SELECT COUNT(*) FROM transformed_employee_data;"
    )
    row_count = records[0][0]

    if row_count == 0:
        raise ValueError("Source table has no data")

    return row_count

# -----------------------
# TASK 2: EXPORT TO PARQUET
# -----------------------
def export_to_parquet(ds):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM transformed_employee_data", engine)

    output_path = f"/opt/airflow/output/employee_data_{ds}.parquet"
    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy"
    )

    return {
        "file_path": output_path,
        "row_count": len(df),
        "file_size_bytes": os.path.getsize(output_path),
    }

# -----------------------
# TASK 3: VALIDATE PARQUET
# -----------------------
def validate_parquet(ds):
    file_path = f"/opt/airflow/output/employee_data_{ds}.parquet"
    df = pd.read_parquet(file_path)

    required_columns = {
        "full_info",
        "age_group",
        "salary_category",
        "year_joined"
    }

    if not required_columns.issubset(set(df.columns)):
        raise ValueError("Parquet file missing required columns")

    if len(df) == 0:
        raise ValueError("Parquet file is empty")

    return True

# -----------------------
# TASK DEFINITIONS
# -----------------------
check_task = PythonOperator(
    task_id="check_source_table",
    python_callable=check_table_exists,
    dag=dag,
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_to_parquet,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag,
)

# -----------------------
# DEPENDENCIES
# -----------------------
check_task >> export_task >> validate_task
