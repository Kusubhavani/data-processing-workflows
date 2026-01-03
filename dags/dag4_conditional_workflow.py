from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datetime import date

dag = DAG(
    dag_id='conditional_workflow_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['conditional', 'branching'],
)

def determine_branch(**context):
    exec_date = context['execution_date'].date()
    weekday_num = exec_date.weekday()  # 0=Mon, 6=Sun
    if weekday_num <= 2:  # Mon-Wed
        return 'weekday_processing'
    elif weekday_num <= 4:  # Thu-Fri
        return 'end_of_week_processing'
    else:  # Sat-Sun
        return 'weekend_processing'

def weekday_process():
    return {
        'day_name': date.today().strftime('%A'),
        'task_type': 'weekday',
        'record_count': 150  # Simulated daily processing
    }

def end_of_week_process():
    return {
        'day_name': date.today().strftime('%A'),
        'task_type': 'end_of_week',
        'weekly_summary': 'Weekly totals processed: 750 records, 5% growth'
    }

def weekend_process():
    return {
        'day_name': date.today().strftime('%A'),
        'task_type': 'weekend',
        'cleanup_status': 'Backup completed, logs archived'
    }

# Start task
start_task = EmptyOperator(task_id='start', dag=dag)

# Branching task
branch_task = BranchPythonOperator(task_id='branch_by_day', python_callable=determine_branch, dag=dag)

# Weekday branch
weekday_task = PythonOperator(task_id='weekday_processing', python_callable=weekday_process, dag=dag)
weekday_summary_task = EmptyOperator(task_id='weekday_summary', dag=dag)

# End of week branch
end_of_week_task = PythonOperator(task_id='end_of_week_processing', python_callable=end_of_week_process, dag=dag)
end_of_week_report_task = EmptyOperator(task_id='end_of_week_report', dag=dag)

# Weekend branch
weekend_task = PythonOperator(task_id='weekend_processing', python_callable=weekend_process, dag=dag)
weekend_cleanup_task = EmptyOperator(task_id='weekend_cleanup', dag=dag)

# End task
end_task = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success', dag=dag)

# Dependencies
start_task >> branch_task
branch_task >> weekday_task >> weekday_summary_task >> end_task
branch_task >> end_of_week_task >> end_of_week_report_task >> end_task
branch_task >> weekend_task >> weekend_cleanup_task >> end_task
