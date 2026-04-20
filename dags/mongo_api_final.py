from datetime import datetime, timedelta
from typing import Any, Tuple

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from dags.utils.mongo_triggers import MongoStatusTrigger

# --- 1. THE DEFERRABLE API OPERATOR ---
class MongoAsyncApiOperator(BaseOperator):
    template_fields: Tuple[str, ...] = ("doc_id",)

    def __init__(
            self,
            conn_id: str,
            http_conn_id: str,
            db_name: str,
            collection: str,
            doc_id: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.http_conn_id = http_conn_id
        self.db_name = db_name
        self.collection = collection
        self.doc_id = doc_id

    def execute(self, context: Context):
        mongo_hook = MongoHook(conn_id=self.conn_id)
        conn = mongo_hook.get_connection(self.conn_id)

        # Manually build a standard MongoDB URI
        # 1. Determine the prefix based on 'srv' extra
        extras = conn.extra_dejson
        prefix = "mongodb+srv" if extras.get("srv") else "mongodb"

        # 2. Extract credentials and host
        user = conn.login
        password = conn.password
        host = conn.host

        # 3. Construct the clean standard URI
        # Note: We exclude the __extra__ portion to avoid Airflow-specific noise
        conn_uri = f"{prefix}://{user}:{password}@{host}/"

        # Append essential MongoDB parameters if they exist in extras
        # Cosmos DB often requires 'ssl=true' and 'retrywrites=false'
        params = []
        if extras.get("ssl"): params.append("ssl=true")
        if extras.get("retrywrites") is False: params.append("retrywrites=false")
        if extras.get("authMechanism"): params.append(f"authMechanism={extras['authMechanism']}")

        if params:
            conn_uri += "?" + "&".join(params)

        self.log.info(f"DEBUG: Passing standard URI to trigger for {self.doc_id}")

        # Trigger your REST API
        http_hook = HttpHook(method='POST', http_conn_id=self.http_conn_id)
        http_hook.run(
            endpoint="/api/process",
            json={"doc_id": self.doc_id, "db": self.db_name, "coll": self.collection}
        )

        self.log.info(f"DEBUG: Passing URI to trigger: {conn_uri}")

        self.defer(
            trigger=MongoStatusTrigger(
                conn_uri=conn_uri,
                db_name=self.db_name,
                collection=self.collection,
                doc_id=self.doc_id
            ),
            method_name="execute_complete"
        )

    def execute_complete(self, context: Context, event: dict):
        """Callback for when the Triggerer detects completion."""
        if event["status"] == "success":
            self.log.info(f"Successfully processed {event['doc_id']}")
            return {"doc_id": event["doc_id"], "success": True}

        raise Exception(f"API Background process failed: {event.get('msg')}")


# --- 2. THE DAG ---
@dag(
    dag_id="mongo_api_final",
    schedule=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,  # Wait 5m, then 10m, and then 20m...
        "max_retry_delay": timedelta(hours=1)
    }
)
def mongo_api_workflow_dag():
    @task
    def fetch_docs():
        """Fetch IDs of documents marked as 'new' from MongoDB."""
        hook = MongoHook(conn_id="mongo_default")
        db = hook.get_conn()["airflow"]
        docs = db["jobs"].find({"status": "new"})
        return [str(d["_id"]) for d in docs]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize_report(results_list: Any):
        """
        Processes results even if some tasks failed.
        Using Any for type-hinting to avoid IDE 'MappedOperator' errors.
        """
        if not results_list:
            print("No results to report.")
            return {"success_count": 0}

        successes = [
            r["doc_id"] for r in results_list
            if isinstance(r, dict) and r.get("success")
        ]

        total = len(results_list)
        success_count = len(successes)
        failed_count = total - success_count

        print(f"--- FINAL REPORT ---")
        print(f"Total documents found: {total}")
        print(f"Successfully processed: {success_count}")
        print(f"Failed after retries: {failed_count}")
        print(f"Successful IDs: {successes}")

        return {"success_count": success_count, "failed_count": failed_count}

    # --- Execution Flow ---

    # 1. Fetch document list
    ids = fetch_docs()

    # 2. Map the Deferrable Operator
    # .output is used to ensure Airflow 3 resolves the XComArg correctly
    process_tasks = MongoAsyncApiOperator.partial(
        task_id="process_doc",
        conn_id="mongo_default",
        http_conn_id="external_airflow_worker",
        db_name="airflow",
        collection="jobs",
        pool="mongo_async_pool"
    ).expand(doc_id=ids)

    # 3. Aggregate results
    finalize_report(process_tasks.output)


# Instantiate the DAG
mongo_api_workflow_dag()