import os
from functools import lru_cache

# Use Environment Variables for actual values
TEST_MAP = {
    "catalog-api-token": os.getenv("CATALOG_API_BEARER", "dev-token-abc"),
    "pricing-token": os.getenv("PRICING_API_BEARER", "dev-token-xyz"),
}

@lru_cache(maxsize=1024)
def resolve_secret(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("kv://"):
        return value

    secret_key = value.replace("kv://", "", 1).strip()
    resolved = TEST_MAP.get(secret_key)

    if resolved is None:
        # Fallback to Airflow Variable if not in TEST_MAP
        from airflow.models import Variable
        resolved = Variable.get(secret_key, default_var=None)

    if resolved is None:
        raise RuntimeError(f"Secret '{secret_key}' could not be resolved.")

    # Only add 'Bearer ' if the header key is 'Authorization' and it's missing
    # Since we don't know the key here, it's safer to just return the value
    # and let the Seeder provide 'Bearer kv://...' if needed.
    return resolved
