from __future__ import annotations

import os
import time
from datetime import datetime, UTC
from pathlib import Path

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from metar_core import (
    load_model,
    aw_fetch_global_most_recent,
    filter_conus_from_aw,
    metar_score,
)

# ----------------------------
# Config via env vars
# ----------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "rarity_model.json")
LENGTH_WEIGHT = float(os.getenv("LENGTH_WEIGHT", "0.02"))
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "45"))

app = FastAPI(title="METAR Leaderboard API", version="0.1.0")

# ----------------------------
# CORS (mainly for Vite dev server mode)
# ----------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Load model once at startup
# ----------------------------
model = load_model(MODEL_PATH)

# Simple in-memory cache for AviationWeather response
_cache = {"ts": 0.0, "data": None}


def _get_aw_data_cached(session: requests.Session):
    now = time.time()
    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_SECONDS:
        return _cache["data"]
    data = aw_fetch_global_most_recent(session)
    _cache["ts"] = now
    _cache["data"] = data
    return data


# ----------------------------
# API routes
# ----------------------------
@app.get("/api/leaderboard")
def leaderboard(
    top: int = Query(25, ge=1, le=200),
    conus: bool = Query(True),
):
    """
    Returns top-N most complex METARs right now, including lat/lon for map plotting.
    """
    with requests.Session() as session:
        metars = _get_aw_data_cached(session)
        filtered = filter_conus_from_aw(metars) if conus else metars

        now = datetime.now(UTC)
        month = now.month

        rows = []
        for m in filtered:
            raw = (m.get("rawOb") or "").strip()
            if not raw:
                continue

            lat = m.get("lat")
            lon = m.get("lon")
            try:
                latf = float(lat) if lat is not None else None
                lonf = float(lon) if lon is not None else None
            except Exception:
                latf, lonf = None, None

            rows.append(
                {
                    "station": (m.get("icaoId") or "----").strip(),
                    "score": metar_score(raw, model, length_weight=LENGTH_WEIGHT, month=month),
                    "metar": raw,
                    "lat": latf,
                    "lon": lonf,
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[:top]

        return {
            "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "top": top,
            "count": len(rows),
            "rows": rows,
        }


@app.get("/api/health")
def health():
    return {"ok": True}


# ----------------------------
# Frontend (React build) serving
# ----------------------------
FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
ASSETS_DIR = FRONTEND_DIST / "assets"
INDEX_HTML = FRONTEND_DIST / "index.html"

# Serve /assets/* directly from the Vite build output
if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def _frontend_available() -> bool:
    return INDEX_HTML.exists()


@app.get("/")
def serve_index():
    if _frontend_available():
        return FileResponse(INDEX_HTML)
    return {"message": "Frontend not built. Run: cd frontend && npm run build"}


# Catch-all: return index.html for SPA routes, but NEVER for /api/*
@app.get("/{full_path:path}")
def spa_fallback(request: Request, full_path: str):
    # Leave API alone
    if full_path.startswith("api/"):
        return FileResponse(INDEX_HTML) if False else ({"detail": "Not Found"}, 404)  # should never hit if API exists

    # If frontend exists, serve SPA entry for any other path
    if _frontend_available():
        return FileResponse(INDEX_HTML)

    return {"message": "Frontend not built. Run: cd frontend && npm run build"}
