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
        if "localhost" in MONGODB_URI or "127.0.0.1" in MONGODB_URI:
            raise RuntimeError(
                "\n========================================================================\n"
                "CRITICAL: Failed to connect to MongoDB at localhost.\n"
                "If you are deploying to Render, you must configure the 'MONGODB_URI'\n"
                "environment variable in the Render Dashboard -> Environment tab\n"
                "to point to your MongoDB Atlas instance.\n"
                "========================================================================"
            ) from e
        else:
            raise RuntimeError(
                f"\n========================================================================\n"
                f"CRITICAL: Failed to connect to MongoDB Atlas.\n"
                f"Please check your MONGODB_URI connection string and ensure that your\n"
                f"database user, password, and IP whitelist (0.0.0.0/0) are correctly set.\n"
                f"Error: {e}\n"
                f"========================================================================"
            ) from e

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
