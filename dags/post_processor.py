from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import get_current_context
from airflow.sdk import Context  # Airflow 3 Context helper

@dag(
    dag_id="post_processing_dag",  # Match the 'target' in your Mongo 'on_complete'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="Processes batch completion results"
)
def post_processor():

    @task
    def process_results():
        # 1. Access the 'conf' passed during the trigger
        # In Airflow 3, you can get this directly from the context
        context = get_current_context()
        conf = context["dag_run"].conf or {}

        batch_id = conf.get("batch_id")
        status = conf.get("status")
        manual_payload = conf.get("manual_payload", {})

        if not batch_id:
            print("No batch_id found in configuration. Manual run?")
            return

        print(f"--- Processing Batch: {batch_id} ---")
        print(f"Status: {status}")
        print(f"Extra Config: {manual_payload}")

        # 2. Your actual business logic here
        # e.g., Update a final report, send a Slack message, etc.
        if status == "success":
            print(f"Success logic for {batch_id}")
        else:
            print(f"Failure logic for {batch_id}")

    process_results()

post_processor()
