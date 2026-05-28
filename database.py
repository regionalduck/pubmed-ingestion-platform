from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGODB_URI, DB_NAME, COLLECTION_NAME, log

_mongo_client = None

def get_client() -> AsyncIOMotorClient:
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = AsyncIOMotorClient(MONGODB_URI)
    return _mongo_client

def get_collection():
    client = get_client()
    return client[DB_NAME][COLLECTION_NAME]

async def init_db():
    client = get_client()
    log.info("Connecting to MongoDB at %s", MONGODB_URI)
    try:
        await client.admin.command("ping")
        log.info("MongoDB connected ✓")
    except Exception as e:
        log.error("MongoDB connection failed: %s", e)

    coll = get_collection()
    await coll.create_index("pmid", unique=True)
    await coll.create_index("search_term")
    await coll.create_index("pub_year")

async def close_db():
    global _mongo_client
    if _mongo_client is not None:
        log.info("Closing MongoDB connection")
        _mongo_client.close()
        _mongo_client = None
