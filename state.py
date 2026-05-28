import threading
from typing import Any, Dict, List
from fastapi import WebSocket

_progress_lock = threading.Lock()
_progress: Dict[str, Any] = {
    "running":       False,
    "search_term":   "",
    "total":         0,   # total on PubMed for this term
    "db_count":      0,   # already in MongoDB before this run
    "fetched":       0,   # PMIDs processed this run
    "new_count":     0,   # truly new articles inserted this run
    "updated_count": 0,   # articles already in DB, refreshed this run
    "status":        "idle",
    "started_at":    None,
    "ended_at":      None,
    "error":         None,
}

class _WSManager:
    def __init__(self):
        self._connections: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket):
        try:
            self._connections.remove(ws)
        except ValueError:
            pass

    async def broadcast(self, data: Dict):
        dead = []
        for ws in self._connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                self._connections.remove(ws)
            except ValueError:
                pass

ws_manager = _WSManager()

def update_progress(**kwargs):
    with _progress_lock:
        _progress.update(kwargs)

async def broadcast_progress():
    with _progress_lock:
        snap = dict(_progress)
    await ws_manager.broadcast(snap)

def get_progress_snapshot() -> Dict[str, Any]:
    with _progress_lock:
        return dict(_progress)

def get_progress_lock():
    return _progress_lock

def increment_progress(fetched: int, new_count: int, updated_count: int, status: str):
    with _progress_lock:
        _progress["fetched"] += fetched
        _progress["new_count"] += new_count
        _progress["updated_count"] += updated_count
        _progress["status"] = status

