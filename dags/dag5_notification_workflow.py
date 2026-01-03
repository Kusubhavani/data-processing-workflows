from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datetime import date

dag = DAG(
    dag_id='notification_workflow',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['notification', 'error_handling'],
)

def send_success_notification(context):
    ti = context['task_instance']
    print(f"SUCCESS: Task {ti.task_id} succeeded at {context['ts']}")
    return {
        'notification_type': 'success',
        'status': 'sent',
        'message': f'Task {ti.task_id} completed successfully',
        'timestamp': context['ts']
    }

def send_failure_notification(context):
    ti = context['task_instance']
    exception = context['exception']
    print(f"FAILURE: Task {ti.task_id} failed: {str(exception)} at {context['ts']}")
    return {
        'notification_type': 'failure',
        'status': 'sent',
        'message': f'Task {ti.task_id} failed',
        'error': str(exception),
        'timestamp': context['ts']
    }

def risky_operation(**context):
    exec_date = context['execution_date'].date()
    day_of_month = exec_date.day
    if day_of_month % 5 == 0:
        raise ValueError(f"Simulated failure on day {day_of_month} (divisible by 5)")
    return {
        'status': 'success',
        'execution_date': exec_date.isoformat(),
        'success': True
    }

def cleanup_task():
    print("Cleanup: Archiving logs and temporary files")
    return {
        'cleanup_status': 'completed',
        'timestamp': datetime.now().isoformat()
    }

# Start task
start_task = EmptyOperator(task_id='start_task', dag=dag)

# Risky operation with callbacks
risky_task = PythonOperator(
    task_id='risky_operation',
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag
)

# Success notification task
success_notification_task = EmptyOperator(
    task_id='success_notification',
    trigger_rule='all_success',
    dag=dag
)

# Failure notification task
failure_notification_task = EmptyOperator(
    task_id='failure_notification',
    trigger_rule='all_failed',
    dag=dag
)

# Always execute cleanup
always_execute_task = PythonOperator(
    task_id='always_execute',
    python_callable=cleanup_task,
    trigger_rule='all_done',
    dag=dag
)

# Dependencies
start_task >> risky_task
risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_execute_task
