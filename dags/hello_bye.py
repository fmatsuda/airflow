from __future__ import annotations

import pendulum

from airflow.sdk import dag, task

@dag(
    dag_id="simple_airflow_3_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # Set to None for manual triggering or a cron expression for scheduling
    catchup=False,
    tags=["example"],
)
def simple_airflow_3dag():
    """
    This DAG demonstrates a simple workflow with two Python tasks.
    """

    @task
    def extract_data():
        """
        Simulates extracting data.
        """
        print("Extracting data...")
        return "Raw Data"

    @task
    def process_data(raw_data: str):
        """
        Simulates processing the extracted data.
        """
        print(f"Processing: {raw_data}")
        processed_data = f"Processed: {raw_data}"
        return processed_data

    # Define the task dependencies
    extracted_data = extract_data()
    processed_output = process_data(raw_data=extracted_data)

# Instantiate the DAG
simple_airflow_3dag()
