from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle


def test_dag1_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag1_task_count():
    dag = DagBag(dag_folder="dags", include_examples=False).dags[
        "csv_to_postgres_ingestion"
    ]
    assert len(dag.tasks) == 3


def test_dag1_task_ids():
    dag = DagBag(dag_folder="dags", include_examples=False).dags[
        "csv_to_postgres_ingestion"
    ]
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {
        "create_table_if_not_exists",
        "truncate_table",
        "load_csv_to_postgres",
    }


def test_dag1_dependencies():
    dag = DagBag(dag_folder="dags", include_examples=False).dags[
        "csv_to_postgres_ingestion"
    ]

    create = dag.get_task("create_table_if_not_exists")
    truncate = dag.get_task("truncate_table")
    load = dag.get_task("load_csv_to_postgres")

    assert truncate in create.downstream_list
    assert load in truncate.downstream_list


def test_dag1_schedule():
    dag = DagBag(dag_folder="dags", include_examples=False).dags[
        "csv_to_postgres_ingestion"
    ]
    assert dag.schedule_interval == "@daily"


def test_dag1_no_cycles():
    dag = DagBag(dag_folder="dags", include_examples=False).dags[
        "csv_to_postgres_ingestion"
    ]
    check_cycle(dag)
