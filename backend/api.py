from __future__ import annotations

import asyncio
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from history_store import offer_rows, get_top
from metar_core import (
    get_last_metar_fetch_debug,
    load_model,
    metar_score,
    taf_score,
    pirep_score,
    aw_fetch_global_most_recent,
    filter_conus_from_aw,
    aw_fetch_taf_most_recent_global,
    aw_fetch_pirep_last_hours_global,
)

MODEL_METAR_PATH = os.getenv("MODEL_PATH", "rarity_model.json")
MODEL_TAF_PATH = os.getenv("TAF_MODEL_PATH", "rarity_taf.json.gz")
MODEL_PIREP_PATH = os.getenv("PIREP_MODEL_PATH", "rarity_pirep.json.gz")

LENGTH_WEIGHT = float(os.getenv("LENGTH_WEIGHT", "0.02"))
TAF_LENGTH_WEIGHT = float(os.getenv("TAF_LENGTH_WEIGHT", "0.01"))
PIREP_LENGTH_WEIGHT = float(os.getenv("PIREP_LENGTH_WEIGHT", "0.005"))

CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "45"))
TAF_CACHE_SECONDS = int(os.getenv("TAF_CACHE_SECONDS", "300"))
PIREP_CACHE_SECONDS = int(os.getenv("PIREP_CACHE_SECONDS", "120"))

API_DEBUG = os.getenv("API_DEBUG", "").strip() not in ("", "0", "false", "False")

# Background history refresh
HISTORY_BG_ENABLED = os.getenv("HISTORY_BG_ENABLED", "1").strip() not in ("", "0", "false", "False")
HISTORY_REFRESH_SECONDS = int(os.getenv("HISTORY_REFRESH_SECONDS", "120"))
HISTORY_BG_TOP = int(os.getenv("HISTORY_BG_TOP", "100"))  # fetch/score top N live; store is capped to 50
HISTORY_BG_CONUS = os.getenv("HISTORY_BG_CONUS", "1").strip() not in ("", "0", "false", "False")
HISTORY_BG_PIREP_HOURS = int(os.getenv("HISTORY_BG_PIREP_HOURS", "24"))

app = FastAPI(title="Aviation Weather Leaderboard API", version="0.5.0")

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

model_metar = load_model(MODEL_METAR_PATH)
model_taf = load_model(MODEL_TAF_PATH) if Path(MODEL_TAF_PATH).exists() else None
model_pirep = load_model(MODEL_PIREP_PATH) if Path(MODEL_PIREP_PATH).exists() else None

_cache_metar: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_taf: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_pirep: Dict[str, Any] = {"ts": 0.0, "data": None, "hours": None}

_bg_task: Optional[asyncio.Task] = None


def _is_nonempty(data: Any) -> bool:
    try:
        return data is not None and hasattr(data, "__len__") and len(data) > 0
    except Exception:
        return data is not None


def _cached_fetch(cache: Dict[str, Any], ttl: int, fetch_fn):
    now = time.time()
    cached = cache.get("data")
    ts = float(cache.get("ts", 0.0) or 0.0)

    if cached is not None and (now - ts) < ttl:
        return cached

    data = fetch_fn()

    if _is_nonempty(data):
        cache["ts"] = now
        cache["data"] = data
        return data

    if _is_nonempty(cached):
        cache["ts"] = now
        return cached

    cache["ts"] = now
    cache["data"] = data
    return data


def _metar_fetch_with_retry():
    with requests.Session() as session:
        data1 = aw_fetch_global_most_recent(session)
    if _is_nonempty(data1):
        return {"data": data1, "attempt_used": 1}

    time.sleep(1.0)

    with requests.Session() as session:
        data2 = aw_fetch_global_most_recent(session)
    return {"data": data2, "attempt_used": 2}


def _score_metars(metars, top: int, conus: bool):
    now = datetime.now(UTC)
    month = now.month
    filtered = filter_conus_from_aw(metars) if conus else metars

    rows = []
    for m in (filtered or []):
        raw = (m.get("rawOb") or m.get("raw") or "").strip()
        if not raw:
            continue

        try:
            latf = float(m.get("lat")) if m.get("lat") is not None else None
            lonf = float(m.get("lon")) if m.get("lon") is not None else None
        except Exception:
            latf, lonf = None, None

        station = (m.get("icaoId") or "----").strip()

        rows.append(
            {
                "product": "METAR",
                "station": station,
                "icaoId": station,
                "score": metar_score(raw, model_metar, length_weight=LENGTH_WEIGHT, month=month),
                "text": raw,
                "rawOb": raw,
                "raw": raw,
                "lat": latf,
                "lon": lonf,
            }
        )

    rows.sort(key=lambda x: x["score"], reverse=True)
    return now, rows[:top], len(metars) if hasattr(metars, "__len__") else None, len(filtered) if hasattr(filtered, "__len__") else None


def _score_tafs(tafs, top: int):
    if model_taf is None:
        return None, []

    now = datetime.now(UTC)
    month = now.month

    rows = []
    for t in (tafs or []):
        raw = (t.get("rawTAF") or t.get("rawOb") or t.get("raw") or "").strip()
        if not raw:
            continue

        try:
            latf = float(t.get("lat")) if t.get("lat") is not None else None
            lonf = float(t.get("lon")) if t.get("lon") is not None else None
        except Exception:
            latf, lonf = None, None

        station = (t.get("stationId") or t.get("icaoId") or t.get("station") or "----").strip()

        rows.append(
            {
                "product": "TAF",
                "station": station,
                "icaoId": station,
                "score": taf_score(raw, model_taf, length_weight=TAF_LENGTH_WEIGHT, month=month),
                "text": raw,
                "raw": raw,
                "lat": latf,
                "lon": lonf,
            }
        )

    rows.sort(key=lambda x: x["score"], reverse=True)
    return now, rows[:top]


def _score_pireps(pireps, top: int):
    if model_pirep is None:
        return None, []

    now = datetime.now(UTC)
    month = now.month

    rows = []
    for p in (pireps or []):
        text = (p.get("raw") or p.get("report") or p.get("text") or p.get("rawOb") or "").strip()
        if not text:
            continue

        try:
            latf = float(p.get("lat")) if p.get("lat") is not None else None
            lonf = float(p.get("lon")) if p.get("lon") is not None else None
        except Exception:
            latf, lonf = None, None

        rows.append(
            {
                "product": "PIREP",
                "station": "PIREP",
                "icaoId": "PIREP",
                "score": pirep_score(text, model_pirep, length_weight=PIREP_LENGTH_WEIGHT, month=month),
                "text": text,
                "raw": text,
                "lat": latf,
                "lon": lonf,
            }
        )

    rows.sort(key=lambda x: x["score"], reverse=True)
    return now, rows[:top]


async def _history_background_loop():
    """
    Periodically fetch + score live data and offer it to the history store,
    so /api/history stays updated even without user traffic.
    """
    # small startup delay so uvicorn fully boots
    await asyncio.sleep(1.0)

    while True:
        started = time.time()
        try:
            # Run network+CPU scoring off the event loop
            await asyncio.to_thread(_history_background_tick)
        except asyncio.CancelledError:
            raise
        except Exception:
            # swallow errors so the loop keeps running
            pass

        elapsed = time.time() - started
        sleep_for = max(5.0, float(HISTORY_REFRESH_SECONDS) - elapsed)
        await asyncio.sleep(sleep_for)


def _history_background_tick():
    top = int(HISTORY_BG_TOP)

    # METAR
    try:
        result = _metar_fetch_with_retry()
        metars = result.get("data")
        if _is_nonempty(metars):
            now, rows, _, _ = _score_metars(metars, top=top, conus=HISTORY_BG_CONUS)
            offer_rows("METAR", rows)
    except Exception:
        pass

    # TAF
    if model_taf is not None:
        try:
            with requests.Session() as session:
                tafs = aw_fetch_taf_most_recent_global(session)
            if _is_nonempty(tafs):
                _, rows = _score_tafs(tafs, top=top)
                offer_rows("TAF", rows)
        except Exception:
            pass

    # PIREP
    if model_pirep is not None:
        try:
            with requests.Session() as session:
                pireps = aw_fetch_pirep_last_hours_global(session, hours=int(HISTORY_BG_PIREP_HOURS))
            if _is_nonempty(pireps):
                _, rows = _score_pireps(pireps, top=top)
                offer_rows("PIREP", rows)
        except Exception:
            pass


@app.on_event("startup")
async def _startup():
    global _bg_task
    if HISTORY_BG_ENABLED and _bg_task is None:
        _bg_task = asyncio.create_task(_history_background_loop())


@app.on_event("shutdown")
async def _shutdown():
    global _bg_task
    if _bg_task is not None:
        _bg_task.cancel()
        try:
            await _bg_task
        except Exception:
            pass
        _bg_task = None


@app.get("/api/health")
def health():
    return {
        "ok": True,
        "models": {
            "metar": str(Path(MODEL_METAR_PATH).resolve()),
            "taf": str(Path(MODEL_TAF_PATH).resolve()) if model_taf else None,
            "pirep": str(Path(MODEL_PIREP_PATH).resolve()) if model_pirep else None,
        },
        "history_bg": {
            "enabled": HISTORY_BG_ENABLED,
            "refresh_seconds": HISTORY_REFRESH_SECONDS,
            "top_scored_each_tick": HISTORY_BG_TOP,
            "metar_conus_only": HISTORY_BG_CONUS,
            "pirep_hours": HISTORY_BG_PIREP_HIREP_HOURS if False else HISTORY_BG_PIREP_HOURS,  # keep simple, avoid refactor
        },
    }


@app.get("/api/history")
def history(product: str = Query("METAR"), top: int = Query(25, ge=1, le=200)):
    p = product.strip().upper()
    if p not in ("METAR", "TAF", "PIREP"):
        return JSONResponse(status_code=400, content={"detail": "product must be METAR, TAF, or PIREP"})
    return get_top(p, top=top)


@app.get("/api/leaderboard")
def leaderboard(top: int = Query(25, ge=1, le=200), conus: bool = Query(True)):
    last_attempt_used = None

    def _fetch():
        nonlocal last_attempt_used
        result = _metar_fetch_with_retry()
        last_attempt_used = result.get("attempt_used")
        return result.get("data")

    metars = _cached_fetch(_cache_metar, CACHE_SECONDS, _fetch)
    now, rows, fetched_count, filtered_count = _score_metars(metars, top=top, conus=conus)

    # Always offer to history (history store is capped to 50/product)
    offer_rows("METAR", rows)

    payload = {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "product": "METAR",
        "top": top,
        "count": len(rows),
        "rows": rows,
    }

    if API_DEBUG:
        payload["debug"] = {
            "cache_ttl_seconds": CACHE_SECONDS,
            "attempt_used": last_attempt_used,
            "fetched": fetched_count,
            "filtered": filtered_count,
            "conus": conus,
            "metar_fetch": get_last_metar_fetch_debug(),
        }

    return payload


@app.get("/api/taf")
def taf_leaderboard(top: int = Query(25, ge=1, le=200)):
    if model_taf is None:
        return JSONResponse(status_code=503, content={"detail": f"TAF model not loaded. Missing file: {MODEL_TAF_PATH}"})

    def _fetch():
        with requests.Session() as session:
            return aw_fetch_taf_most_recent_global(session)

    tafs = _cached_fetch(_cache_taf, TAF_CACHE_SECONDS, _fetch)

    now, rows = _score_tafs(tafs, top=top)
    offer_rows("TAF", rows)

    return {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "product": "TAF",
        "top": top,
        "count": len(rows),
        "rows": rows,
    }


@app.get("/api/pirep")
def pirep_leaderboard(top: int = Query(25, ge=1, le=200), hours: int = Query(24, ge=1, le=72)):
    if model_pirep is None:
        return JSONResponse(status_code=503, content={"detail": f"PIREP model not loaded. Missing file: {MODEL_PIREP_PATH}"})

    now_ts = time.time()
    cached_ok = (
        _cache_pirep.get("data") is not None
        and _cache_pirep.get("hours") == hours
        and (now_ts - float(_cache_pirep.get("ts", 0.0))) < PIREP_CACHE_SECONDS
    )

    if cached_ok:
        pireps = _cache_pirep["data"]
    else:
        with requests.Session() as session:
            data = aw_fetch_pirep_last_hours_global(session, hours=hours)
        if _is_nonempty(data) or not _is_nonempty(_cache_pirep.get("data")):
            _cache_pirep["data"] = data
        _cache_pirep["ts"] = now_ts
        _cache_pirep["hours"] = hours
        pireps = _cache_pirep["data"]

    now, rows = _score_pireps(pireps, top=top)
    offer_rows("PIREP", rows)

    return {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "product": "PIREP",
        "top": top,
        "hours": hours,
        "count": len(rows),
        "rows": rows,
    }


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
