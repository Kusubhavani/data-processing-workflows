from airflow.models import DagBag


def test_all_dags_loaded():
    """Ensure all expected DAGs are present."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)

    expected_dags = {
        "csv_to_postgres_ingestion",
        "data_transformation_pipeline",
        "postgres_to_parquet_export",
        "conditional_workflow_pipeline",
        "notification_workflow",
    }

    assert expected_dags.issubset(set(dagbag.dags.keys()))


def test_no_import_errors():
    """Ensure no DAG import errors exist."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dagbag.import_errors) == 0


def test_unique_dag_ids():
    """Ensure all DAG IDs are unique."""
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag_ids = [dag.dag_id for dag in dagbag.dags.values()]
    assert len(dag_ids) == len(set(dag_ids))
