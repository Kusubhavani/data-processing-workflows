from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import numpy as np

dag = DAG(
    dag_id='data_transformation_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'transformation'],
)

def create_transformed_table():
    hook = PostgresHook(postgres_conn_id='postgres')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transformed_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            joindate DATE,
            fullinfo VARCHAR(500),
            agegroup VARCHAR(20),
            salarycategory VARCHAR(20),
            yearjoined INTEGER
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def transform_data():
    hook = PostgresHook(postgres_conn_id='postgres')
    df = hook.get_pandas_df(table='raw_employee_data')
    
    # Transformations per spec
    df['fullinfo'] = df['name'] + ' - ' + df['city']
    df['agegroup'] = np.select(
        [df['age'] < 30, df['age'] < 50, df['age'] >= 50],
        ['Young', 'Mid', 'Senior'],
        default='Senior'
    )
    df['salarycategory'] = np.select(
        [df['salary'] < 50000, df['salary'] < 80000, df['salary'] >= 80000],
        ['Low', 'Medium', 'High'],
        default='High'
    )
    df['yearjoined'] = pd.to_datetime(df['joindate']).dt.year.astype(int)
    
    rows_processed = len(df)
    
    # Truncate for idempotency, then append
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE transformed_employee_data;")
    conn.commit()
    cur.close()
    conn.close()
    
    df.to_sql(
        name='transformed_employee_data',
        con=hook.get_sqlalchemy_engine(),
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    return {'rows_processed': rows_processed, 'rows_inserted': rows_processed}

create_transformed_table_task = PythonOperator(
    task_id='create_transformed_table',
    python_callable=create_transformed_table,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id='transform_and_load',
    python_callable=transform_data,
    dag=dag,
)

create_transformed_table_task >> transform_and_load_task
