from datetime import datetime, timedelta, timezone
from typing import Dict, List
from airflow.hooks.base import BaseHook
from airflow.sdk import dag
from airflow.exceptions import AirflowSkipException
from pymongo import MongoClient
import requests
from airflow.sdk import Asset
from airflow.decorators import task as task_deco
from airflow.utils.trigger_rule import TriggerRule
from bson import json_util
import json
from airflow.exceptions import AirflowFailException


MONGO_ASSET = Asset("mongo://jobs/created")

def _db(mongo_uri, mongo_db):
    return MongoClient(mongo_uri)[mongo_db]


@task_deco
def get_runnable_job_ids(**context) -> List[Dict]:
    # 1. Get the full map of triggering events
    event_map = context.get('triggering_asset_events', {})
    # 2. Check for the Mongo Asset specifically
    mongo_events = context.get('triggering_asset_events', {}).get(MONGO_ASSET)
    if not mongo_events:
        raise AirflowSkipException("No triggering Mongo events")

    required_set = {'mongo_uri', 'mongo_db', 'mongo_job_ids_query_limit', 'batch_id', 'job_id'}
    # 3. Iterate through events (there could be multiple if queued)
    jobs: List[dict] = []
    for event in mongo_events:
        # 4. Retrieve the 'extra' dictionary passed via API
        extra = event.extra or {}
        missing = required_set - extra.keys()
        if missing:
            print(f"[get_runnable_job_ids] Missing event keys: {missing}")
            raise AirflowFailException(f"Validation Failed: missing key in extra {missing}")

        job = {
                "mongo_uri": extra["mongo_uri"],
                "mongo_db": extra["mongo_db"],
                "batch_id": extra["batch_id"],
                "job_id": extra["job_id"],
                "query_limit": extra["mongo_job_ids_query_limit"],
        }
        jobs.append(job)
        print(f"[get_runnable_job_ids] job: {job}")


    # claim ownership once per run
    first = jobs[0]
    extra_mongo_uri = first['mongo_uri']
    extra_mongo_db = first['mongo_db']
    extra_mongo_job_ids_query_limit = first['query_limit']

    db = _db(extra_mongo_uri, extra_mongo_db)
    now = datetime.now(timezone.utc)
    stale_threshold = now - timedelta(minutes=5)
    # 1. IDENTIFY THE DATABASE
    db_name = db.name
    coll_names = db.list_collection_names()

    # 2. PROBE THE JOBS COLLECTION
    total_in_coll = db.jobs.count_documents({})
    all_statuses = db.jobs.distinct("status")

    # 3. PRINT FOR LOGS
    print(f"--- AIRFLOW MONGO PROBE ---")
    print(f"Target Database: {db_name}")
    print(f"Collections Found: {coll_names}")
    print(f"Total Documents in 'jobs': {total_in_coll}")
    print(f"Unique Statuses: {all_statuses}")
    print(f"---------------------------")

    # ATOMIC RECOVERY & CLAIM:
    # Grab 'created' jobs OR 'running' jobs that have timed out
    res = db.jobs.update_many(
        {
            "$or": [
                {"status": "created"},
                {
                    "status": {"$in": ["claimed", "running"]},
                    "last_heartbeat": {"$lt": stale_threshold}
                }
            ]
        },
        {"$set": {
            "status": "claimed",
            "last_heartbeat": now,
            "claimed_by_dag_run": context['dag_run'].run_id
        }, "$currentDate": {"updated_at": True}}
    )

    if res.modified_count == 0:
        raise AirflowSkipException("No new or stale jobs found.")

    # Fetch the batch / job IDs for processing
    job_cursor = db.jobs.find(
        {"status": "claimed", "claimed_by_dag_run": context['dag_run'].run_id},
        {"batch_id": 1, "job_id": 1}
    ).sort("started_at", 1)
    return [{"mongo_uri": extra_mongo_uri, "mongo_db": extra_mongo_db,
             "mongo_job_ids_query_limit": extra_mongo_job_ids_query_limit,
             "batch_id": j["batch_id"], "job_id": j["job_id"]}
            for j in job_cursor]


@task_deco
def claim_and_generate_waves(job_ref: Dict) -> List[Dict]:
    """Claims the job and INFERS the SKUs to process from the job_ids collection."""
    bid, jid, mongo_uri, mongo_db = job_ref["batch_id"], job_ref["job_id"], job_ref["mongo_uri"], job_ref["mongo_db"]
    mongo_job_ids_query_limit = job_ref["mongo_job_ids_query_limit"]
    db = _db(mongo_uri, mongo_db)
    job_doc = db.jobs.find_one({"batch_id": bid, "job_id": jid}, {"status": 1})
    job_status = job_doc.get("status", 1)
    print(f"[claim_and_generate_waves] batch: {bid}, id: {jid}, status: {job_status}, "
          f"mongo_job_ids_query_limit: {mongo_job_ids_query_limit}")

    # 1. Atomic Claim
    res = db.jobs.update_one(
        {"batch_id": bid, "job_id": jid, "status": "claimed"},
        {"$set": {"status": "running", "started_at": datetime.now(timezone.utc)},
         "$currentDate": {"updated_at": True}}
    )
    if res.modified_count == 0:
        print(f"[claim_and_generate_waves] WARNING: Job {bid}/{jid} not found in 'claimed' state.")
        return []

    # 2. INFER: Fetch SKU specs directly from job_ids
    # We query for 'pending' to allow for partial retries if needed - Control upper limit for waves
    sku_cursor = db.job_ids.find(
        {"batch_id": bid, "job_id": jid, "status": "pending"}
    ).limit(mongo_job_ids_query_limit)
    specs = []  # Fetch full docs for Power BI context (future)
    # Fetch full docs for Power BI context (future)
    for s in sku_cursor:
        # This automatically converts ObjectIds, Dates, and Decimals to
        # JSON-safe strings or dictionaries
        safe_doc = json.loads(json_util.dumps(s))
        specs.append(safe_doc)
    print(f"[claim_and_generate_waves] batch: {bid}, id: {jid}, status: {job_status}, specs: {specs}")

    # 3. Get batch_size from the Job doc (which no longer needs the 'ids' list)
    job_doc = db.jobs.find_one({"batch_id": bid, "job_id": jid}, {"batch_size": 1, "status": 1})
    batch_size = int(job_doc.get("batch_size", 1)) if job_doc else 1
    print(f"[claim_and_generate_waves] batch: {bid}, id: {jid}, status: {job_status}, batch_size: {batch_size}")

    from dags.utils.chunking import chunk_list
    waves = chunk_list(specs, batch_size)
    print(f"[claim_and_generate_waves] batch: waves: {waves}")

    return [{"mongo_uri": mongo_uri, "mongo_db": mongo_db, "batch_id": bid, "job_id": jid, "wave": w} for w in waves]


@task_deco
def flatten_queue(nested: List[List[Dict]]) -> List[Dict]:
    # DEBUG: See what we are actually receiving
    print(f"DEBUG: Nested waves received: {nested}")
    flat_list = [item for sublist in nested for item in sublist if sublist]
    print(f"DEBUG: Flattened queue length: {len(flat_list)}")

    if not flat_list:
        # Instead of skipping silently, let's see why it's empty
        print("WARNING: Work queue is empty. process_wave will skip!")

    return flat_list


@task_deco
def analyze_workload(work_queue: List[Dict]) -> str:
    """
    Calculates Cost (API count) and Priority for every wave.
    Returns two lists: one for pools, one for priority weights.
    """
    pools = []
    priorities = []

    for unit in work_queue:
        wave = unit.get("wave", [])
        # Total HTTP requests this specific worker will perform
        api_count = sum(len(sku.get("apis", [])) for sku in wave)

        # 1. Route to Pool based on 'Heaviness'
        if api_count > 1000:
            pool = "heavy_load_pool"
            weight = 1  # Lowest priority (get in back of line)
        elif api_count > 200:
            pool = "standard_pool"
            weight = 10
        else:
            pool = "light_load_pool"
            weight = 50  # Highest priority (jump to front)

        pools.append(pool)
        priorities.append(weight)

    json_string =  json.dumps({"pools": pools, "priorities":  priorities})
    print(f"[def analyze_workload] return: {json_string} data type: {type(json_string)}")
    return json_string


@task_deco
def prepare_wave_config(waves: list, pools: list) -> list[dict]:
    """
    Standard Python zip to pair 8 waves with 8 pools.
    Returns: [{'work_unit': ..., 'pool': ...}, ...]
    """
    # Standard Python zip is 100% reliable
    return [{"work_unit": w, "pool": p} for w, p in zip(waves, pools)]


@task_deco
def get_workload(workload_string, key):
    print(f"ACTUAL workload_dict: {workload_string}")
    workload_dict = json.loads(workload_string)
    if key in workload_dict:
        return workload_dict[key]
    else:
        return None


@task_deco(max_active_tis_per_dagrun=16, execution_timeout=timedelta(minutes=15), weight_rule="absolute")
def process_wave(work_unit: Dict, pool: str):
    #db = _db()
    bid, jid, wave, mongo_uri, mongo_db = (work_unit["batch_id"], work_unit["job_id"], work_unit["wave"],
                                           work_unit["mongo_uri"], work_unit["mongo_db"])
    db = _db(mongo_uri, mongo_db)
    # Unique Wave ID for visibility in Mongo
    #wave_id = f"wave_{datetime.now(timezone.utc).strftime('%H%M%S')}_{bid[:3]}_{jid[-3:]}"
    wave_id = f"wave_{datetime.now(timezone.utc).strftime('%H%M%S')}_{bid}_{jid}"
    # Note: Airflow handles the actual "slot" reservation before this function runs.
    print(f"[process_wave] Running in pool: {pool} with absolute weight logic.")
    any_failed = False

    for spec in wave:
        sku_id = spec["id"]

        # Update SKU status AND Job heartbeat simultaneously
        now = datetime.now(timezone.utc)
        res = db.job_ids.update_one(
            {"batch_id": bid, "job_id": jid, "id": sku_id, "status": "pending"},
            {"$set": {"status": "running", "wave_id": wave_id, "started_at": now},
             "$currentDate": {"updated_at": True}}
        )
        if res.modified_count == 0:
            print(f"Skipping batch_id: {bid}, job_id: {jid} sku_id: {sku_id} - already picked up by another worker.")
            return

        # Pulse the Job heartbeat to prevent recovery logic from stealing it
        db.jobs.update_one(
            {"batch_id": bid, "job_id": jid},
            {"$set": {"last_heartbeat": now},
             "$currentDate": {"updated_at": True}}
        )

        try:
            for api in spec.get("apis", []):
                # Apply DEFAULT timeout if none provided
                timeout = api.get("timeout") or 10
                # (Your execute_api logic should consume this timeout)
                from dags.utils.http_executor import execute_api
                execute_api({**api, "timeout": timeout}, bid, jid, sku_id)

            db.job_ids.update_one(
                {"batch_id": bid, "job_id": jid, "id": sku_id},
                {"$set": {"status": "completed", "completed_at": datetime.now(timezone.utc)},
                 "$currentDate": {"updated_at": True}}
            )
        except Exception as e:
            db.job_ids.update_one(
                {"batch_id": bid, "job_id": jid, "id": sku_id},
                {"$set": {
                    "status": "failed",
                    "completed_at": datetime.now(timezone.utc),
                    "error": str(e)
                }}
            )
            any_failed = True  # Continue the loop for remaining SKUs in wave

    if any_failed:
        raise RuntimeError(f"Wave {wave_id} in {bid}/{jid} had partial failures.")


@task_deco(trigger_rule=TriggerRule.ALL_DONE, retries=3, retry_delay=timedelta(minutes=1))
def finalize_job(job_ref: Dict):
    """
    Finalizes a Job and its parent Batch.
    Dispatches success/failure callbacks via Airflow Assets or External APIs.
    """
    #db = _db()
    now = datetime.now(timezone.utc)
    bid, jid, mongo_uri, mongo_db = job_ref["batch_id"], job_ref["job_id"], job_ref["mongo_uri"], job_ref["mongo_db"]
    db = _db(mongo_uri, mongo_db)
    # --- PAGINATION CHECK (from imposed limit of mongo_job_ids_query_limit in sku_cursor = db.job_ids.find(
    #         {"batch_id": bid, "job_id": jid, "status": "pending"},
    #     ).limit(mongo_job_ids_query_limit)) in claim_and_generate_waves
    # Check if this job still has SKUs waiting for the next run that gets triggered from bridge ---
    pending_count = db.job_ids.count_documents({"batch_id": bid, "job_id": jid, "status": "pending"})

    if pending_count > 0:
        print(f"[finalize] ⏳ Job {bid}/{jid} has {pending_count} SKUs pending. Skipping finalization for this run.")
        return  # EXIT EARLY: Keep status as 'running' for the Bridge to pulse

    # 1. Update individual Job Status based on SKU results
    skus = list(db.job_ids.find({"batch_id": bid, "job_id": jid}))
    if not skus:
        print(f"[finalize] No SKUs found for Batch: {bid}, Job: {jid}")
        return

    any_sku_failed = any(s.get("status") == "failed" for s in skus)
    all_skus_done = all(s.get("status") == "completed" for s in skus)
    if all_skus_done and not any_sku_failed:
        job_final_status = "completed"
    else:
        job_final_status = "failed"  # or "incomplete"

    db.jobs.update_one(
        {"batch_id": bid, "job_id": jid},
        {"$set": {"status": job_final_status, "completed_at": now},
         "$currentDate": {"updated_at": True}}
    )
    print(f"[finalize] Batch {bid} | Job {jid} marked as {job_final_status}")

    # 2. Check batch (simplified)
    total_jobs_in_batch = db.jobs.count_documents({"batch_id": bid})
    finished_jobs_count = db.jobs.count_documents({
        "batch_id": bid,
        "status": {"$in": ["completed", "failed"]}
    })

    # 3. If all jobs are done, finalize the Batch and trigger callbacks
    if finished_jobs_count >= total_jobs_in_batch:
        # ATOMIC LOCK: Only the FIRST finalizer to update this flag triggers the API
        res = db.batches.update_one(
            {
                "batch_id": bid,
                "callback_emitted": {"$ne": True} # Gatekeeper condition
            },
            {"$set": {
                "callback_emitted": True,
                "callback_emitted_at": now
            },
                "$currentDate": {"updated_at": True}}
        )
        # THE EXIT: If you didn't flip the flag, you are a redundant task. STOP HERE.
        if res.modified_count == 0:
            print(f"[finalize] 🛑 Batch {bid} already handled by another instance. Exiting.")
            return

        # Check if ANY job in this batch failed to determine outcome
        any_job_failed = db.jobs.find_one({"batch_id": bid, "status": "failed"})
        batch_final_outcome = "failure" if any_job_failed else "success"

        # Update the FINAL status of the batch
        db.batches.update_one(
            {"batch_id": bid},
            {"$set": {
                "status": "completed" if batch_final_outcome == "success" else "failed",
                "completed_at": now
            }, "$currentDate": {"updated_at": True}}
        )

        print(f"[finalize] 🎯 First responder for {bid}. Outcome: {batch_final_outcome}")


        # 4. DISPATCHER: Execute the 'on_complete' instructions
        callbacks = db.batches.find_one({"batch_id": bid}).get("on_complete", {})
        action = callbacks.get(batch_final_outcome)
        if not action: return

        if action:
            action_type = action.get("type")
            target = action.get("target")
            print(f"[finalize] action_type: {action_type}")
            print(f"[finalize] target: {target}")

            # SCENARIO A: Dag (Triggers another DAG)
            if action_type == "dag":
                # 1. Fetch credentials from the 'airflow_api' connection
                conn = BaseHook.get_connection("airflow_api")
                base_url = conn.host.rstrip('/')

                # 2. Get JWT Token (Airflow 3 Handshake)
                # Endpoint: /auth/token
                auth_url = f"{base_url}/auth/token"
                auth_payload = {"username": conn.login, "password": conn.password}

                print(f"[finalize] auth_url: {auth_url}")
                print(f"[finalize] auth_payload: {auth_payload}")
                auth_res = requests.post(auth_url, json=auth_payload, timeout=10)
                auth_res.raise_for_status()
                token = auth_res.json().get("access_token")  # Extract JWT token
                print(f"[finalize] Auth token: {token}")

                # 3. Trigger DAG via API v2
                # Endpoint: /api/v2/dags/{dag_id}/dagRuns
                trigger_url = f"{base_url}/api/v2/dags/{target}/dagRuns"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                }
                dag_run_id = f"batch_{bid}_{datetime.now(timezone.utc).strftime('%y%m%d%H%M%S%f')}"
                now_iso = datetime.now(timezone.utc).isoformat()
                payload = {
                    "logical_date": now_iso,
                    "conf": {
                        "batch_id": bid,
                        "status": batch_final_outcome,
                        "manual_payload": action.get("conf", {})
                    },
                    "dag_run_id": dag_run_id
                }

                print(f"[finalize] Trigger URL: {trigger_url}")
                print(f"[finalize] Payload: {payload}")
                print(f"[finalize] Triggering {target} via API v2")
                resp = requests.post(trigger_url, json=payload, headers=headers, timeout=10)
                print(f"[finalize] details: {resp.json()}")
                resp.raise_for_status()

                # Construct the deep link to the Airflow UI
                # This link takes you to this specific run
                logs_url = f"{base_url}/dags/{target}/runs/{dag_run_id}"

                # Save to MongoDB Batch document
                db.batches.update_one(
                    {"batch_id": bid},
                    {"$set": {
                        "callback_dag_logs_url": logs_url,
                        "triggered_dag_run_id": dag_run_id,
                        "last_callback_at": datetime.now(timezone.utc)
                    }, "$currentDate": {"updated_at": True}}
                )


            # SCENARIO B: External API Callback (Webhook/Notification)
            elif action_type == "api":
                payload = action.get("payload", {})
                # Merge original payload with run metadata
                full_payload = {
                    **payload,
                    "batch_id": bid,
                    "outcome": batch_final_outcome,
                    "timestamp": now.isoformat()
                }
                print(f"[finalize] Full payload: {full_payload}")
                try:
                    from dags.utils.http_executor import execute_api
                    action["url"] = target
                    action["json"] = full_payload
                    print(f"[finalize] Sending API Callback to: {target}")
                    resp = execute_api({**action}, bid, job_id="", item_id="")
                    print(f"[finalize] API Callback successful (HTTP {resp})")
                except Exception as e:
                    print(f"[finalize] API Callback failed: {e}")

            else:
                print(f"[finalize] Unknown callback type: {action_type}")
        else:
            print(f"[finalize] No callback configured for outcome: {batch_final_outcome}")
    else:
        remaining = total_jobs_in_batch - finished_jobs_count
        print(f"[finalize] Batch {bid} still has {remaining} job(s) running.")


@dag(dag_id="job_runner", start_date=datetime(2025, 1, 1),
     schedule=[MONGO_ASSET], catchup=False, max_active_runs=1, render_template_as_native_obj=True)
def job_runner():
    runnable_jobs = get_runnable_job_ids()

    # 1. Expand jobs into waves
    nested_waves = claim_and_generate_waves.expand(job_ref=runnable_jobs)

    # 2. Flatten for the worker queue
    work_queue = flatten_queue(nested_waves)

    # 3. Get dynamic routing data
    workload = analyze_workload(work_queue)
    pool_list = get_workload(workload, "pools")

    # 4. Process waves (Parallel) to avoid the cross-product
    mapped_input = prepare_wave_config(work_queue, pool_list)

    # Use expand_kwargs to map the zipped pairs (mapped_input) to avoid cartesian cross-product
    # cartesian cross-product: (number of work_unit) X (number of pool_list)
    # this statement: processed = process_wave.expand(work_unit=work_queue, pool=pool_list)
    # creates a cross-product issue
    processed = process_wave.expand_kwargs(mapped_input)

    # 5. Finalize (Fan-in)
    # 'processed >>' is used to ensure the finalizer waits for ALL process_wave instances
    finalizer = finalize_job.expand(job_ref=runnable_jobs)

    var = processed >> finalizer


job_runner()
