# pyrefly: ignore [missing-import]
import httpx
from typing import Dict, List
# pyrefly: ignore [missing-import]
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import TOOL_NAME, NCBI_EMAIL, NCBI_API_KEY, ESEARCH_URL, EFETCH_URL

def _ncbi_params(**extra) -> Dict:
    params = {
        "tool":   TOOL_NAME,
        "email":  NCBI_EMAIL,
        "retmode": "json",
    }
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    params.update(extra)
    return params

@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
async def _get(client: httpx.AsyncClient, url: str, params: Dict) -> httpx.Response:
    resp = await client.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp

async def get_total_count(client: httpx.AsyncClient, term: str, mindate: str = None, maxdate: str = None) -> int:
    extra = {}
    if mindate and maxdate:
        extra["datetype"] = "pdat"
        extra["mindate"] = mindate
        extra["maxdate"] = maxdate
    params = _ncbi_params(db="pubmed", term=term, retmax=0, **extra)
    params["retmode"] = "json"
    resp = await _get(client, ESEARCH_URL, params)
    data = resp.json()
    return int(data["esearchresult"]["count"])

async def get_pmids(client: httpx.AsyncClient, term: str, retstart: int, retmax: int, mindate: str = None, maxdate: str = None) -> List[str]:
    extra = {}
    if mindate and maxdate:
        extra["datetype"] = "pdat"
        extra["mindate"] = mindate
        extra["maxdate"] = maxdate
    params = _ncbi_params(db="pubmed", term=term, retstart=retstart, retmax=retmax, **extra)
    params["retmode"] = "json"
    resp = await _get(client, ESEARCH_URL, params)
    data = resp.json()
    return data["esearchresult"].get("idlist", [])

async def fetch_xml(client: httpx.AsyncClient, pmids: List[str]) -> bytes:
    params = _ncbi_params(db="pubmed", id=",".join(pmids), rettype="xml")
    params.pop("retmode", None)
    resp = await _get(client, EFETCH_URL, params)
    return resp.content
