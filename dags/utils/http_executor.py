import requests
import time
from .secrets import resolve_secret
import random


def execute_api(api: dict, batch_id: str, job_id: str, item_id: str):
    # 1. Resolve secrets and apply defaults
    headers = {k: resolve_secret(v) for k, v in (api.get("headers") or {}).items()}
    retries = int(api.get("retries", 0))
    # Use the timeout provided, or default to 30
    timeout = int(api.get("timeout") or 30)
    verify = api.get("verify", True)
    method = api.get("method", "GET").upper()
    retry_delay = int(api.get("retry_delay") or 1)
    if retry_delay < 0:
        retry_delay = 1
    print(f"[execute_api] {api}")
    for attempt in range(1, retries + 2):
        try:
            print(f"[execute_api] Batch:{batch_id} Job:{job_id} SKU:{item_id} "
                  f"| Attempt {attempt}/{retries + 1} | {method} {api['url']}")

            resp = requests.request(
                method=method,
                url=api["url"],
                headers=headers,
                json=api.get("json"),
                data=api.get("data"),
                timeout=timeout,
                verify=verify
            )
            resp.raise_for_status()
            return resp.text

        except requests.exceptions.RequestException as e:
            print(f"[execute_api] SKU:{item_id} Attempt {attempt} FAILED: {e}")
            if attempt > retries:
                raise
            # Small backoff before retry to be kind to the API
            # Calculate jittered delay (full jitter)
            # Delay is a random number between 0 and 2^attempt * base_delay
            # A common approach uses a random value between 0 and the current backoff

            # Simplified approach using a simple doubling of the delay with added random
            # current_delay = base_delay * (2**attempt) # Classic exponential backoff
            # jittered_delay = current_delay + random.uniform(0, 1) # Simple jitter

            # A more effective full jitter strategy:
            # The next wait time should be a random value between 0 and the current cap

            max_jitter_delay = retry_delay * (2 ** attempt)
            sleep_time = random.uniform(0, max_jitter_delay)

            print(f"[execute_api] Attempt {attempt} failed. Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)  # Use time.sleep() with the calculated random delay

