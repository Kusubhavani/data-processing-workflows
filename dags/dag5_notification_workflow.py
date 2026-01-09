from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# -----------------------
# DAG DEFINITION
# -----------------------
dag = DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

# -----------------------
# CALLBACK FUNCTIONS
# -----------------------
def send_success_notification(context):
    return {
        "notification_type": "success",
        "message": f"Task {context['task_instance'].task_id} succeeded",
        "execution_date": str(context["execution_date"])
    }

def send_failure_notification(context):
    return {
        "notification_type": "failure",
        "message": f"Task {context['task_instance'].task_id} failed",
        "execution_date": str(context["execution_date"])
    }

# -----------------------
# RISKY TASK
# -----------------------
def risky_operation(**context):
    day = context["execution_date"].day

    # Fail if day is divisible by 5
    if day % 5 == 0:
        raise Exception("Simulated failure condition met")

    return {
        "status": "success",
        "day": day
    }

# -----------------------
# CLEANUP TASK
# -----------------------
def cleanup():
    return {
        "cleanup_status": "completed",
        "timestamp": str(datetime.utcnow())
    }

# -----------------------
# TASK DEFINITIONS
# -----------------------
start = EmptyOperator(task_id="start", dag=dag)

risky_task = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag,
)

success_notification = EmptyOperator(
    task_id="success_notification",
    trigger_rule="all_success",
    dag=dag,
)

failure_notification = EmptyOperator(
    task_id="failure_notification",
    trigger_rule="all_failed",
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_task",
    python_callable=cleanup,
    trigger_rule="all_done",
    dag=dag,
)

# -----------------------
# DEPENDENCIES
# -----------------------
start >> risky_task >> [success_notification, failure_notification]
[success_notification, failure_notification] >> cleanup_task
