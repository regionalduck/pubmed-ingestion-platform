import os
import logging
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pubmed")

load_dotenv()

MONGODB_URI      = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME          = os.getenv("DB_NAME", "pubmed_db")
COLLECTION_NAME  = os.getenv("COLLECTION_NAME", "articles")
NCBI_EMAIL       = os.getenv("NCBI_EMAIL", "user@example.com")
TOOL_NAME        = os.getenv("TOOL_NAME", "pubmed_ingestion_tool")
NCBI_API_KEY     = os.getenv("NCBI_API_KEY", "").strip()
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "100"))
# 0 = no limit (fetch everything PubMed returns)
MAX_RESULTS      = int(os.getenv("MAX_RESULTS_LIMIT", "0"))

ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
EFETCH_URL  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
SLEEP_TIME  = 0.11 if NCBI_API_KEY else 0.34
