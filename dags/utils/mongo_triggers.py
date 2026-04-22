import asyncio
from typing import Any, AsyncIterator, Dict, Tuple
from airflow.triggers.base import BaseTrigger, TriggerEvent
from motor.motor_asyncio import AsyncIOMotorClient

_CLIENT_CACHE = {}

async def get_mongo_client(uri: str):
    if uri not in _CLIENT_CACHE:
        _CLIENT_CACHE[uri] = AsyncIOMotorClient(uri, maxPoolSize=100)
    return _CLIENT_CACHE[uri]

class MongoStatusTrigger(BaseTrigger):
    def __init__(self, conn_uri: str, db_name: str, collection: str, doc_id: str, poke_interval: int = 15):
        super().__init__()
        self.conn_uri, self.db_name, self.collection = conn_uri, db_name, collection
        self.doc_id, self.poke_interval = doc_id, poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        # Add 'dags.' to the front of the string
        return ("dags.utils.mongo_triggers.MongoStatusTrigger", {
            "conn_uri": self.conn_uri,
            "db_name": self.db_name,
            "collection": self.collection,
            "doc_id": self.doc_id,
            "poke_interval": self.poke_interval
        })

    async def run(self) -> AsyncIterator[TriggerEvent]:
        client = await get_mongo_client(self.conn_uri)
        coll = client[self.db_name][self.collection]
        while True:
            doc = await coll.find_one({"_id": self.doc_id})
            if doc and doc.get("status") == "completed":
                yield TriggerEvent({"status": "success", "doc_id": self.doc_id})
                return
            elif doc and doc.get("status") == "failed":
                yield TriggerEvent({"status": "error", "doc_id": self.doc_id, "msg": doc.get("error_msg")})
                return
            await asyncio.sleep(self.poke_interval)