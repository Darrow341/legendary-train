from __future__ import annotations

import os
import time
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Dict

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from metar_core import (
    load_model,
    metar_score,
    taf_score,
    pirep_score,
    aw_fetch_global_most_recent,
    filter_conus_from_aw,
    aw_fetch_taf_most_recent_global,
    aw_fetch_taf_most_recent_conus,
    aw_fetch_pirep_last_hours_global,
)

# ----------------------------
# Config via env vars
# ----------------------------
MODEL_METAR_PATH = os.getenv("MODEL_PATH", "rarity_model.json")
MODEL_TAF_PATH = os.getenv("TAF_MODEL_PATH", "rarity_taf.json.gz")
MODEL_PIREP_PATH = os.getenv("PIREP_MODEL_PATH", "rarity_pirep.json.gz")

LENGTH_WEIGHT = float(os.getenv("LENGTH_WEIGHT", "0.02"))
TAF_LENGTH_WEIGHT = float(os.getenv("TAF_LENGTH_WEIGHT", "0.01"))
PIREP_LENGTH_WEIGHT = float(os.getenv("PIREP_LENGTH_WEIGHT", "0.005"))

CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "45"))
TAF_CACHE_SECONDS = int(os.getenv("TAF_CACHE_SECONDS", "300"))
PIREP_CACHE_SECONDS = int(os.getenv("PIREP_CACHE_SECONDS", "120"))

app = FastAPI(title="Aviation Weather Leaderboard API", version="0.2.1")

# ----------------------------
# CORS (Vite dev server)
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
# Load models once at startup
# ----------------------------
model_metar = load_model(MODEL_METAR_PATH)
model_taf = load_model(MODEL_TAF_PATH) if Path(MODEL_TAF_PATH).exists() else None
model_pirep = load_model(MODEL_PIREP_PATH) if Path(MODEL_PIREP_PATH).exists() else None

# ----------------------------
# Simple in-memory caches
# ----------------------------
_cache_metar: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_taf: Dict[str, Any] = {"ts": 0.0, "data": None, "conus": None}
_cache_pirep: Dict[str, Any] = {"ts": 0.0, "data": None, "hours": None}


def _cached_fetch(cache: Dict[str, Any], ttl: int, fetch_fn):
    now = time.time()
    if cache.get("data") is not None and (now - float(cache.get("ts", 0.0))) < ttl:
        return cache["data"]
    data = fetch_fn()
    cache["ts"] = now
    cache["data"] = data
    return data


# ----------------------------
# API routes
# ----------------------------
@app.get("/api/health")
def health():
    return {
        "ok": True,
        "models": {
            "metar": str(Path(MODEL_METAR_PATH).resolve()),
            "taf": str(Path(MODEL_TAF_PATH).resolve()) if model_taf else None,
            "pirep": str(Path(MODEL_PIREP_PATH).resolve()) if model_pirep else None,
        },
    }


@app.get("/api/leaderboard")
def leaderboard(
    top: int = Query(25, ge=1, le=200),
    conus: bool = Query(True),
):
    """
    METAR leaderboard (AviationWeather mostRecent).
    """
    with requests.Session() as session:

        def _fetch():
            return aw_fetch_global_most_recent(session)

        metars = _cached_fetch(_cache_metar, CACHE_SECONDS, _fetch)
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
                    "product": "METAR",
                    "station": (m.get("icaoId") or "----").strip(),
                    "score": metar_score(raw, model_metar, length_weight=LENGTH_WEIGHT, month=month),
                    "text": raw,
                    "lat": latf,
                    "lon": lonf,
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[:top]

        return {
            "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "product": "METAR",
            "top": top,
            "count": len(rows),
            "rows": rows,
        }


@app.get("/api/taf")
def taf_leaderboard(
    top: int = Query(25, ge=1, le=200),
    conus: bool = Query(True),
):
    """
    TAF leaderboard (AviationWeather mostRecent).

    Default is CONUS-only via bbox filter.
    """
    if model_taf is None:
        return JSONResponse(
            status_code=503,
            content={"detail": f"TAF model not loaded. Missing file: {MODEL_TAF_PATH}"},
        )

    with requests.Session() as session:

        def _fetch_for(conus_flag: bool):
            if conus_flag:
                return aw_fetch_taf_most_recent_conus(session)
            return aw_fetch_taf_most_recent_global(session)

        # Cache depends on conus flag
        now_ts = time.time()
        if (
            _cache_taf.get("data") is not None
            and _cache_taf.get("conus") == conus
            and (now_ts - float(_cache_taf.get("ts", 0.0))) < TAF_CACHE_SECONDS
        ):
            tafs = _cache_taf["data"]
        else:
            tafs = _fetch_for(conus)
            _cache_taf["ts"] = now_ts
            _cache_taf["data"] = tafs
            _cache_taf["conus"] = conus

        now = datetime.now(UTC)
        month = now.month

        rows = []
        for t in tafs:
            raw = (t.get("rawTAF") or t.get("rawOb") or t.get("raw") or "").strip()
            if not raw:
                continue

            lat = t.get("lat")
            lon = t.get("lon")
            try:
                latf = float(lat) if lat is not None else None
                lonf = float(lon) if lon is not None else None
            except Exception:
                latf, lonf = None, None

            station = (t.get("stationId") or t.get("icaoId") or t.get("station") or "----").strip()

            rows.append(
                {
                    "product": "TAF",
                    "station": station,
                    "score": taf_score(raw, model_taf, length_weight=TAF_LENGTH_WEIGHT, month=month),
                    "text": raw,
                    "lat": latf,
                    "lon": lonf,
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[:top]

        return {
            "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "product": "TAF",
            "top": top,
            "conus": conus,
            "count": len(rows),
            "rows": rows,
        }


@app.get("/api/pirep")
def pirep_leaderboard(
    top: int = Query(25, ge=1, le=200),
    hours: int = Query(24, ge=1, le=72),
):
    """
    PIREP leaderboard (AviationWeather last N hours).
    """
    if model_pirep is None:
        return JSONResponse(
            status_code=503,
            content={"detail": f"PIREP model not loaded. Missing file: {MODEL_PIREP_PATH}"},
        )

    with requests.Session() as session:

        def _fetch():
            return aw_fetch_pirep_last_hours_global(session, hours=hours)

        # cache depends on hours
        now_ts = time.time()
        if (
            _cache_pirep.get("data") is not None
            and _cache_pirep.get("hours") == hours
            and (now_ts - float(_cache_pirep.get("ts", 0.0))) < PIREP_CACHE_SECONDS
        ):
            pireps = _cache_pirep["data"]
        else:
            pireps = _fetch()
            _cache_pirep["ts"] = now_ts
            _cache_pirep["data"] = pireps
            _cache_pirep["hours"] = hours

        now = datetime.now(UTC)
        month = now.month

        rows = []
        for p in pireps:
            text = (p.get("raw") or p.get("report") or p.get("text") or p.get("rawOb") or "").strip()
            if not text:
                continue

            lat = p.get("lat")
            lon = p.get("lon")
            try:
                latf = float(lat) if lat is not None else None
                lonf = float(lon) if lon is not None else None
            except Exception:
                latf, lonf = None, None

            rows.append(
                {
                    "product": "PIREP",
                    "station": "PIREP",
                    "score": pirep_score(text, model_pirep, length_weight=PIREP_LENGTH_WEIGHT, month=month),
                    "text": text,
                    "lat": latf,
                    "lon": lonf,
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[:top]

        return {
            "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "product": "PIREP",
            "top": top,
            "hours": hours,
            "count": len(rows),
            "rows": rows,
        }


# ----------------------------
# Frontend (React build) serving
# ----------------------------
FRONTEND_DIST = Path(__file__).resolve().parents[1] / "frontend" / "dist"
ASSETS_DIR = FRONTEND_DIST / "assets"
INDEX_HTML = FRONTEND_DIST / "index.html"

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


def _frontend_available() -> bool:
    return INDEX_HTML.exists()


@app.get("/")
def serve_index():
    if _frontend_available():
        return FileResponse(INDEX_HTML)
    return {"message": "Frontend not built. Run: cd frontend && npm run build"}


@app.get("/{full_path:path}")
def spa_fallback(request: Request, full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse(status_code=404, content={"detail": "Not Found"})
    if _frontend_available():
        return FileResponse(INDEX_HTML)
    return {"message": "Frontend not built. Run: cd frontend && npm run build"}
