from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

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

app = FastAPI(title="Aviation Weather Leaderboard API", version="0.3.3")

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
    last_attempt_used = None

    def _fetch():
        nonlocal last_attempt_used
        result = _metar_fetch_with_retry()
        last_attempt_used = result.get("attempt_used")
        return result.get("data")

    metars = _cached_fetch(_cache_metar, CACHE_SECONDS, _fetch)
    filtered = filter_conus_from_aw(metars) if conus else metars

    now = datetime.now(UTC)
    month = now.month

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
    rows = rows[:top]

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
            "fetched": len(metars) if hasattr(metars, "__len__") else None,
            "filtered": len(filtered) if hasattr(filtered, "__len__") else None,
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
    rows = rows[:top]

    payload = {
        "generated_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "product": "TAF",
        "top": top,
        "count": len(rows),
        "rows": rows,
    }
    return payload


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
    rows = rows[:top]

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
