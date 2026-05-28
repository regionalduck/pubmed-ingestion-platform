import asyncio
import httpx
from datetime import datetime, timezone
from pymongo import UpdateOne

from config import BATCH_SIZE, MAX_RESULTS, SLEEP_TIME, log
from database import get_collection
from state import (
    update_progress,
    broadcast_progress,
    increment_progress,
    get_progress_snapshot,
)
from ncbi_client import get_total_count, get_pmids, fetch_xml
from xml_parser import parse_xml_batch

async def fetch_all_pubmed(
    search_term: str,
    limit: int = 5000,
    mindate: str = None,
    maxdate: str = None,
    avail_abstract: bool = False,
    avail_free_full: bool = False,
    avail_full: bool = False
):
    collection = get_collection()

    # Snapshot of how many articles we already have for this term
    existing_db_count = await collection.count_documents({"search_term": search_term})

    update_progress(
        running=True,
        search_term=search_term,
        total=0,
        db_count=existing_db_count,
        fetched=0,
        new_count=0,
        updated_count=0,
        status="Starting…",
        started_at=datetime.now(timezone.utc).isoformat(),
        ended_at=None,
        error=None,
    )
    await broadcast_progress()

    timestamp = datetime.now(timezone.utc).isoformat()

    # Build NCBI search term by appending filters
    ncbi_term = search_term
    if avail_abstract:
        ncbi_term += " AND hasabstract"
    if avail_free_full:
        ncbi_term += " AND free full text[filter]"
    if avail_full:
        ncbi_term += " AND full text[sb]"

    try:
        async with httpx.AsyncClient() as client:
            # Step 1 – count total on PubMed
            log.info("Counting results for: %r (mindate: %s, maxdate: %s, ncbi_term: %s)", search_term, mindate, maxdate, ncbi_term)
            pubmed_total = await get_total_count(client, ncbi_term, mindate=mindate, maxdate=maxdate)

            # Apply limit if positive (otherwise use config/pubmed_total)
            total = pubmed_total if limit <= 0 else min(pubmed_total, limit)
            log.info("PubMed total: %d | Will fetch: %d | Already in DB: %d",
                     pubmed_total, total, existing_db_count)

            update_progress(
                total=total,
                status=f"PubMed has {pubmed_total:,} articles — {existing_db_count:,} already in DB — starting ingestion…"
            )
            await broadcast_progress()

            if total == 0:
                update_progress(
                    status="No results found.",
                    running=False,
                    ended_at=datetime.now(timezone.utc).isoformat()
                )
                await broadcast_progress()
                return

            # Determine dynamic batch size based on limit/total
            dynamic_batch_size = min(BATCH_SIZE, total) if total > 0 else BATCH_SIZE

            # Step 2 – batch loop
            for i in range(0, total, dynamic_batch_size):
                start = existing_db_count + i
                batch_num = i // dynamic_batch_size + 1
                log.info("Batch %d | retstart=%d", batch_num, start)

                update_progress(status=f"Batch {batch_num} — fetching PMIDs…")
                await broadcast_progress()

                pmids = await get_pmids(client, ncbi_term, start, min(dynamic_batch_size, total - i), mindate=mindate, maxdate=maxdate)
                if not pmids:
                    continue

                await asyncio.sleep(SLEEP_TIME)

                update_progress(status=f"Batch {batch_num} — downloading XML…")
                await broadcast_progress()

                xml_bytes = await fetch_xml(client, pmids)
                await asyncio.sleep(SLEEP_TIME)

                docs = parse_xml_batch(xml_bytes, search_term, timestamp)
                log.info("Batch %d — parsed %d articles", batch_num, len(docs))

                if docs:
                    now_iso = datetime.now(timezone.utc).isoformat()
                    ops = [
                        UpdateOne(
                            {"pmid": d["pmid"]},
                            {
                                # Always overwrite all fields except created_at
                                "$set": {**d, "updated_at": now_iso},
                                # Only write created_at on first insert
                                "$setOnInsert": {"created_at": now_iso},
                            },
                            upsert=True,
                        )
                        for d in docs
                    ]
                    result = await collection.bulk_write(ops, ordered=False)

                    newly_inserted = result.upserted_count
                    refreshed      = result.modified_count

                    # Get updated snapshot values thread-safely
                    snap = get_progress_snapshot()
                    next_fetched = len(pmids)
                    total_fetched = snap['fetched'] + next_fetched
                    total_new = snap['new_count'] + newly_inserted
                    total_refreshed = snap['updated_count'] + refreshed

                    status_msg = (
                        f"Batch {batch_num} done — "
                        f"{total_fetched:,}/{total:,} processed · "
                        f"{total_new:,} new · {total_refreshed:,} refreshed"
                    )

                    increment_progress(
                        fetched=next_fetched,
                        new_count=newly_inserted,
                        updated_count=refreshed,
                        status=status_msg
                    )
                    await broadcast_progress()

        # Final DB count for this term
        final_db_count = await collection.count_documents({"search_term": search_term})
        update_progress(
            db_count=final_db_count,
            status="Completed ✓",
            ended_at=datetime.now(timezone.utc).isoformat()
        )
        snap = get_progress_snapshot()
        log.info(
            "Ingestion complete for %r — new: %d | refreshed: %d | total in DB: %d",
            search_term, snap["new_count"], snap["updated_count"], final_db_count
        )

    except Exception as exc:
        log.error("Ingestion error: %s", exc, exc_info=True)
        update_progress(
            status=f"Error: {exc}",
            error=str(exc),
            ended_at=datetime.now(timezone.utc).isoformat()
        )

    finally:
        update_progress(running=False)
        await broadcast_progress()
