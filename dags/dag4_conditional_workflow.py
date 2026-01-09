from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# -----------------------
# DAG DEFINITION
# -----------------------
dag = DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

# -----------------------
# BRANCH DECISION
# -----------------------
def determine_branch(**context):
    execution_date = context["execution_date"]
    day_of_week = execution_date.weekday()  # 0=Mon, 6=Sun

    if day_of_week <= 2:
        return "weekday_processing"
    elif day_of_week <= 4:
        return "end_of_week_processing"
    else:
        return "weekend_processing"

# -----------------------
# TASK FUNCTIONS
# -----------------------
def weekday_process():
    return {
        "task_type": "weekday",
        "message": "Running weekday processing"
    }

def end_of_week_process():
    return {
        "task_type": "end_of_week",
        "message": "Running end-of-week processing"
    }

def weekend_process():
    return {
        "task_type": "weekend",
        "message": "Running weekend processing"
    }

# -----------------------
# TASK DEFINITIONS
# -----------------------
start = EmptyOperator(task_id="start", dag=dag)

branch = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=determine_branch,
    dag=dag,
)

weekday_task = PythonOperator(
    task_id="weekday_processing",
    python_callable=weekday_process,
    dag=dag,
)

end_of_week_task = PythonOperator(
    task_id="end_of_week_processing",
    python_callable=end_of_week_process,
    dag=dag,
)

weekend_task = PythonOperator(
    task_id="weekend_processing",
    python_callable=weekend_process,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

# -----------------------
# DEPENDENCIES
# -----------------------
start >> branch
branch >> weekday_task >> end
branch >> end_of_week_task >> end
branch >> weekend_task >> end
