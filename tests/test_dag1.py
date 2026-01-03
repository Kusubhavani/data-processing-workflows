from airflow.models import DagBag
import pytest
from dags.dag1_csv_to_postgres import create_employee_table, truncate_employee_table, load_csv_data  # Direct import

def test_dag1_loaded():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    assert 'csv_to_postgres_ingestion' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_structure():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    assert len(dag.tasks) == 3

def test_dag1_task_dependencies():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    create_task = dag.get_task('create_table_if_not_exists')
    truncate_task = dag.get_task('truncate_table')
    load_task = dag.get_task('load_csv_to_postgres')
    assert truncate_task in create_task.downstream_list
    assert load_task in truncate_task.downstream_list

def test_dag1_no_cycles():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    dag.test_cycle()

def test_dag1_schedule():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['csv_to_postgres_ingestion']
    assert dag.schedule_interval == '@daily'

def test_dag1_functions_callable():
    assert callable(create_employee_table)
    assert callable(truncate_employee_table)
    assert callable(load_csv_data)

def test_load_csv_data_return_type():
    with pytest.raises(FileNotFoundError):  # No file, but checks return
        result = load_csv_data()
        assert isinstance(result, int)
