from airflow.models import DagBag
import pytest
from dags.dag2_data_transformation import create_transformed_table, transform_data

def test_dag2_loaded():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    assert 'data_transformation_pipeline' in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag2_structure():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    assert len(dag.tasks) == 2

def test_dag2_task_dependencies():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    create_task = dag.get_task('create_transformed_table')
    transform_task = dag.get_task('transform_and_load')
    assert transform_task in create_task.downstream_list

def test_dag2_no_cycles():
    dagbag = DagBag(dag_folder='../dags/', include_examples=False)
    dag = dagbag.dags['data_transformation_pipeline']
    dag.test_cycle()

def test_dag2_functions():
    assert callable(create_transformed_table)
    # transform_data needs DB, skip full or mock
