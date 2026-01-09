from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

def create_transformed_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INT,
        name TEXT,
        age INT,
        city TEXT,
        salary NUMERIC,
        joindate DATE,
        full_info TEXT,
        age_group TEXT,
        salary_category TEXT,
        year_joined INT
    );
    """
    hook.run(sql)


def transform_and_load():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM raw_employee_data", engine)

    if df.empty:
        print("No data found in raw_employee_data")
        return

    # Transformations
    df["full_info"] = df["name"] + " - " + df["city"]

    df["age_group"] = df["age"].apply(
        lambda x: "Young" if x < 30 else "Mid" if x < 50 else "Senior"
    )

    df["salary_category"] = df["salary"].apply(
        lambda x: "Low" if x < 50000 else "Medium" if x < 80000 else "High"
    )

    # ✅ CORRECT COLUMN NAME
    df["year_joined"] = pd.to_datetime(df["joindate"]).dt.year

    df.to_sql(
        "transformed_employee_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"Inserted {len(df)} rows into transformed_employee_data")


with DAG(
    dag_id="data_transformation_pipeline_v2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table = PythonOperator(
        task_id="create_transformed_table",
        python_callable=create_transformed_table,
    )

    transform_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,
    )

    create_table >> transform_load
