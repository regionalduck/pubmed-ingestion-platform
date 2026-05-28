"""
NCBI PubMed Ingestion Platform
FastAPI backend entry point with WebSocket-based progress and UI routes.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict

import httpx
from fastapi import APIRouter, BackgroundTasks, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from config import log
from database import init_db, close_db, get_collection
from state import ws_manager, get_progress_lock, get_progress_snapshot
from ncbi_client import get_total_count
from ingestion import fetch_all_pubmed

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield
    await close_db()

app = FastAPI(
    title="NCBI PubMed Ingestion Platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Serve UI at both root and /pubmed
@app.get("/", response_class=HTMLResponse)
@app.get("/pubmed", response_class=HTMLResponse)
async def serve_ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# API and WebSocket routes prefixed with /pubmed
api_router = APIRouter(prefix="/pubmed")

def get_date_range(pub_date_filter: str, custom_start_date: str = None, custom_end_date: str = None):
    """
    Returns (mindate, maxdate) formatted as YYYY/MM/DD based on the publication date filter.
    """
    now = datetime.now()
    maxdate = now.strftime("%Y/%m/%d")

    if pub_date_filter == "1y":
        try:
            mindate = now.replace(year=now.year - 1).strftime("%Y/%m/%d")
        except ValueError:
            mindate = (now - timedelta(days=365)).strftime("%Y/%m/%d")
        return mindate, maxdate
    elif pub_date_filter == "5y":
        try:
            mindate = now.replace(year=now.year - 5).strftime("%Y/%m/%d")
        except ValueError:
            mindate = (now - timedelta(days=5 * 365)).strftime("%Y/%m/%d")
        return mindate, maxdate
    elif pub_date_filter == "10y":
        try:
            mindate = now.replace(year=now.year - 10).strftime("%Y/%m/%d")
        except ValueError:
            mindate = (now - timedelta(days=10 * 365)).strftime("%Y/%m/%d")
        return mindate, maxdate
    elif pub_date_filter == "custom":
        start = custom_start_date.replace("-", "/") if custom_start_date else "1900/01/01"
        end = custom_end_date.replace("-", "/") if custom_end_date else maxdate
        return start, end
    return None, None

@api_router.post("/search")
async def start_search(
    background_tasks: BackgroundTasks,
    search_term: str = Query(..., min_length=1, max_length=500, description="PubMed search term"),
    pub_date_filter: str = Query("all", description="Publication date filter (all, 1y, 5y, 10y, custom)"),
    custom_start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    custom_end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(5000, ge=1, description="Max articles to ingest"),
    avail_abstract: bool = Query(False, description="Filter: Abstract available"),
    avail_free_full: bool = Query(False, description="Filter: Free Full Text available"),
    avail_full: bool = Query(False, description="Filter: Full Text available"),
):
    lock = get_progress_lock()
    snap = get_progress_snapshot()
    with lock:
        if snap["running"]:
            raise HTTPException(
                status_code=409,
                detail=f"Ingestion already running for '{snap['search_term']}'. Please wait.",
            )

    clean_term = search_term.strip()
    log.info("New ingestion requested: %r (limit: %d, date_filter: %s, abstract: %s, free_full: %s, full: %s)",
             clean_term, limit, pub_date_filter, avail_abstract, avail_free_full, avail_full)

    mindate, maxdate = get_date_range(pub_date_filter, custom_start_date, custom_end_date)

    background_tasks.add_task(
        fetch_all_pubmed,
        clean_term,
        limit,
        mindate,
        maxdate,
        avail_abstract,
        avail_free_full,
        avail_full
    )
    return {"message": f"Ingestion started for '{clean_term}'", "search_term": clean_term}

@api_router.get("/progress")
async def get_progress():
    return get_progress_snapshot()

@api_router.get("/check")
async def check_term(
    search_term: str = Query(..., min_length=1, max_length=500, description="PubMed search term to check"),
    pub_date_filter: str = Query("all", description="Publication date filter (all, 1y, 5y, 10y, custom)"),
    custom_start_date: str = Query(None, description="Start date (YYYY-MM-DD)"),
    custom_end_date: str = Query(None, description="End date (YYYY-MM-DD)"),
    avail_abstract: bool = Query(False, description="Filter: Abstract available"),
    avail_free_full: bool = Query(False, description="Filter: Free Full Text available"),
    avail_full: bool = Query(False, description="Filter: Full Text available"),
):
    clean_term = search_term.strip()
    coll = get_collection()

    mindate, maxdate = get_date_range(pub_date_filter, custom_start_date, custom_end_date)

    # Build NCBI search term by appending filters
    ncbi_term = clean_term
    if avail_abstract:
        ncbi_term += " AND hasabstract"
    if avail_free_full:
        ncbi_term += " AND free full text[filter]"
    if avail_full:
        ncbi_term += " AND full text[sb]"

    try:
        async with httpx.AsyncClient() as client:
            pubmed_total = await get_total_count(client, ncbi_term, mindate=mindate, maxdate=maxdate)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NCBI lookup failed: {exc}")

    db_count  = await coll.count_documents({"search_term": clean_term})
    remaining = max(0, pubmed_total - db_count)

    return {
        "search_term":  clean_term,
        "pubmed_total": pubmed_total,
        "db_count":     db_count,
        "remaining":    remaining,
    }

@api_router.websocket("/ws/progress")
async def websocket_progress(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        # Send current state immediately on connect
        await ws.send_json(get_progress_snapshot())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)

@api_router.get("/articles")
async def list_articles(
    search_term: str  = Query(None, description="Filter by ingestion search term"),
    year_start:  str  = Query(None, description="Filter by publication year (start)"),
    year_end:    str  = Query(None, description="Filter by publication year (end)"),
    keyword:     str  = Query(None, description="Filter by keyword (partial match)"),
    q:           str  = Query(None, description="Full-text search on title/abstract"),
    page:        int  = Query(1, ge=1),
    page_size:   int  = Query(20, ge=1, le=100),
):
    coll   = get_collection()
    filt: Dict = {}

    if search_term:
        filt["search_term"] = search_term
    if year_start or year_end:
        year_filter = {}
        if year_start:
            year_filter["$gte"] = year_start
        if year_end:
            year_filter["$lte"] = year_end
        filt["pub_year"] = year_filter
    if keyword:
        filt["keywords"] = {"$elemMatch": {"$regex": keyword, "$options": "i"}}
    if q:
        filt["$or"] = [
            {"title":    {"$regex": q, "$options": "i"}},
            {"abstract": {"$regex": q, "$options": "i"}},
        ]

    skip  = (page - 1) * page_size
    total = await coll.count_documents(filt)
    docs  = await coll.find(filt, {"_id": 0}).skip(skip).limit(page_size).sort("ingested_at", -1).to_list(None)

    return {
        "total":     total,
        "page":      page,
        "page_size": page_size,
        "pages":     (total + page_size - 1) // page_size,
        "articles":  docs,
    }

@api_router.get("/stats")
async def get_stats():
    coll = get_collection()
    total = await coll.count_documents({})

    # Articles by search term
    by_term_cursor = coll.aggregate([
        {"$group": {"_id": "$search_term", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 20},
    ])
    by_term = [{"term": d["_id"], "count": d["count"]} async for d in by_term_cursor]

    # Articles by year
    by_year_cursor = coll.aggregate([
        {"$match": {"pub_year": {"$ne": ""}}},
        {"$group": {"_id": "$pub_year", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}},
        {"$limit": 20},
    ])
    by_year = [{"year": d["_id"], "count": d["count"]} async for d in by_year_cursor]

    # Top journals
    by_journal_cursor = coll.aggregate([
        {"$match": {"journal": {"$ne": ""}}},
        {"$group": {"_id": "$journal", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
    ])
    by_journal = [{"journal": d["_id"], "count": d["count"]} async for d in by_journal_cursor]

    return {
        "total_articles": total,
        "by_search_term": by_term,
        "by_year":        by_year,
        "by_journal":     by_journal,
    }

@api_router.get("/search-terms")
async def list_search_terms():
    coll   = get_collection()
    terms  = await coll.distinct("search_term")
    return {"terms": sorted(t for t in terms if t)}

# Register router
app.include_router(api_router)
