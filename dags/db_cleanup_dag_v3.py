import pendulum
from datetime import timedelta
from airflow.decorators import dag, task

# Import the database cleanup utility from the Airflow core
from airflow.utils.db_cleanup import run_cleanup

@dag(
    dag_id="db_cleanup_dag_v3",
    start_date=pendulum.datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["maintenance", "airflow3"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def db_cleanup_dag_v3():
    """
    A maintenance DAG to clean up the Airflow metadata database using
    the recommended method for Airflow 3.0.
    """

    @task
    def cleanup_task():
        """
        Executes the database cleanup function directly in a Python task.
        """
        # Set the maximum age of metadata to keep in days
        MAX_DB_ENTRY_AGE_IN_DAYS = 30
        clean_before_timestamp = pendulum.today().subtract(
            days=MAX_DB_ENTRY_AGE_IN_DAYS
        )

        # Execute the database cleanup.
        # confirm=False is required for automated execution.
        # skip_archive=True prevents the creation of archive tables.
        run_cleanup(
            clean_before_timestamp=clean_before_timestamp,
            skip_archive=True,
            confirm=False,
        )

    cleanup_task()