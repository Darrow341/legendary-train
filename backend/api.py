# backend/api.py
from __future__ import annotations

import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

# --- Local modules (your repo has these) ---
from . import history_store as hs
from .metar_core import (
    load_model,
    metar_score,
    taf_score,
    pirep_score,
    aw_fetch_global_most_recent,
    filter_conus_from_aw,
    aw_fetch_taf_most_recent_global,
    aw_fetch_pirep_last_hours_global,
)

# ----------------------------
# Paths / configuration
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent


def _resolve(path: str) -> str:
    p = Path(path)
    return str(p if p.is_absolute() else (BASE_DIR / p).resolve())


MODEL_METAR_PATH = _resolve(os.getenv("MODEL_PATH", "rarity_model.json"))
MODEL_TAF_PATH = _resolve(os.getenv("TAF_MODEL_PATH", "rarity_taf.json.gz"))
MODEL_PIREP_PATH = _resolve(os.getenv("PIREP_MODEL_PATH", "rarity_pirep.json.gz"))

LENGTH_WEIGHT = float(os.getenv("LENGTH_WEIGHT", "0.02"))
TAF_LENGTH_WEIGHT = float(os.getenv("TAF_LENGTH_WEIGHT", "0.01"))
PIREP_LENGTH_WEIGHT = float(os.getenv("PIREP_LENGTH_WEIGHT", "0.005"))

CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "45"))
TAF_CACHE_SECONDS = int(os.getenv("TAF_CACHE_SECONDS", "300"))
PIREP_CACHE_SECONDS = int(os.getenv("PIREP_CACHE_SECONDS", "120"))

# Radar
RADAR_TILE_TTL_SECONDS = int(os.getenv("RADAR_TILE_TTL_SECONDS", "30"))
NOAA_RADAR_MAPSERVER = os.getenv(
    "NOAA_RADAR_MAPSERVER",
    "https://mapservices.weather.noaa.gov/eventdriven/rest/services/radar/radar_base_reflectivity/MapServer",
)

# History DB (used only if history_store exposes a HistoryStore class)
DEFAULT_DB_PATH = BASE_DIR / "data" / "history.sqlite3"
HISTORY_DB_PATH = Path(os.getenv("HISTORY_DB_PATH", str(DEFAULT_DB_PATH)))

# ----------------------------
# History adapter (supports your history_store.py regardless of API shape)
# ----------------------------
class _HistoryAdapter:
    """
    Works with either:
      - hs.HistoryStore(db_path).offer_rows(product, rows) / .get_top(product, top=...)
      - module-level functions offer_rows / get_top
      - module-level functions with similar names (add_rows, record_rows, top, etc.)
    """

    def __init__(self) -> None:
        self._store = None
        if hasattr(hs, "HistoryStore"):
            try:
                self._store = hs.HistoryStore(HISTORY_DB_PATH)  # type: ignore[attr-defined]
            except Exception:
                self._store = None

    def offer_rows(self, product: str, rows: List[Dict[str, Any]]) -> None:
        # class-based
        if self._store is not None and hasattr(self._store, "offer_rows"):
            try:
                self._store.offer_rows(product, rows)  # type: ignore[misc]
                return
            except Exception:
                return

        # function-based
        for name in ("offer_rows", "add_rows", "record_rows", "insert_rows"):
            fn = getattr(hs, name, None)
            if callable(fn):
                try:
                    fn(product, rows)  # type: ignore[misc]
                    return
                except Exception:
                    return

    def get_top(self, product: str, top: int) -> List[Dict[str, Any]]:
        # class-based
        if self._store is not None and hasattr(self._store, "get_top"):
            try:
                return self._store.get_top(product, top=top)  # type: ignore[misc]
            except Exception:
                return []

        # function-based
        for name in ("get_top", "top", "fetch_top", "get_history_top"):
            fn = getattr(hs, name, None)
            if callable(fn):
                try:
                    try:
                        return fn(product, top=top)  # type: ignore[misc]
                    except TypeError:
                        return fn(product, top)  # type: ignore[misc]
                except Exception:
                    return []

        return []


history = _HistoryAdapter()

# ----------------------------
# App
# ----------------------------
app = FastAPI(title="METAR/TAF/PIREP Complexity API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------
# Load models at startup
# ----------------------------
model_metar = load_model(MODEL_METAR_PATH)
model_taf = load_model(MODEL_TAF_PATH) if Path(MODEL_TAF_PATH).exists() else None
model_pirep = load_model(MODEL_PIREP_PATH) if Path(MODEL_PIREP_PATH).exists() else None

# ----------------------------
# Caches
# ----------------------------
_cache_metar: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_taf: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_pirep: Dict[str, Any] = {"ts": 0.0, "data": None, "hours": None}

# Radar tile cache: key -> (ts, bytes)
_radar_tile_cache: Dict[str, Tuple[float, bytes]] = {}

_WEBMERCATOR_R = 6378137.0
_WEBMERCATOR_MAX = 20037508.342789244


def _cached_fetch(cache: Dict[str, Any], ttl: int, fetch_fn):
    now = time.time()
    if cache.get("data") is not None and (now - float(cache.get("ts", 0.0))) < ttl:
        return cache["data"]
    data = fetch_fn()
    cache["ts"] = now
    cache["data"] = data
    return data


# ----------------------------
# Radar helpers
# ----------------------------
def _tile_to_bbox_3857(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    n = 2**z
    x0, x1 = x / n, (x + 1) / n
    y0, y1 = y / n, (y + 1) / n

    lon_left, lon_right = x0 * 360.0 - 180.0, x1 * 360.0 - 180.0

    def inv_mercator_lat(t: float) -> float:
        return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * t))))

    lat_top, lat_bottom = inv_mercator_lat(y0), inv_mercator_lat(y1)

    def lonlat_to_3857(lon: float, lat: float) -> Tuple[float, float]:
        lon_rad = math.radians(lon)
        lat_rad = math.radians(max(min(lat, 89.999999), -89.999999))
        mx = _WEBMERCATOR_R * lon_rad
        my = _WEBMERCATOR_R * math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
        mx = max(min(mx, _WEBMERCATOR_MAX), -_WEBMERCATOR_MAX)
        my = max(min(my, _WEBMERCATOR_MAX), -_WEBMERCATOR_MAX)
        return mx, my

    xmin, ymax = lonlat_to_3857(lon_left, lat_top)
    xmax, ymin = lonlat_to_3857(lon_right, lat_bottom)
    return xmin, ymin, xmax, ymax


def _fetch_noaa_radar_tile_png(z: int, x: int, y: int, size: int = 256) -> bytes:
    xmin, ymin, xmax, ymax = _tile_to_bbox_3857(z, x, y)
    export_url = f"{NOAA_RADAR_MAPSERVER}/export"

    params = {
        "f": "image",
        "bbox": f"{xmin},{ymin},{xmax},{ymax}",
        "bboxSR": "3857",
        "imageSR": "3857",
        "size": f"{size},{size}",
        "format": "png32",
        "transparent": "true",
        "layers": "show:3",
        "_ts": str(int(time.time() * 1000)),
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "image/*,*/*;q=0.8",
        "Referer": "https://radar.weather.gov/",
        "Origin": "https://radar.weather.gov",
    }

    r = requests.get(export_url, params=params, headers=headers, timeout=15)
    r.raise_for_status()
    return r.content


# ----------------------------
# API routes
# ----------------------------
@app.get("/api/health")
def api_health():
    return {
        "ok": True,
        "history_db": str(HISTORY_DB_PATH),
        "models": {
            "metar": str(Path(MODEL_METAR_PATH).resolve()),
            "taf": str(Path(MODEL_TAF_PATH).resolve()) if model_taf else None,
            "pirep": str(Path(MODEL_PIREP_PATH).resolve()) if model_pirep else None,
        },
        "radar_mapserver": NOAA_RADAR_MAPSERVER,
    }


@app.get("/api/history")
def api_history(
    product: str = Query(..., description="METAR, TAF, or PIREP"),
    top: int = Query(25, ge=1, le=200),
):
    p = product.strip().upper()
    if p not in {"METAR", "TAF", "PIREP"}:
        return JSONResponse(status_code=400, content={"detail": f"Unknown product: {product}"})
    return {"rows": history.get_top(p, top=int(top))}


# ✅ IMPORTANT: radar route must be defined BEFORE any SPA catch-all route
@app.api_route("/api/radar/tiles/{z}/{x}/{y}.png", methods=["GET", "HEAD"])
def radar_tile(z: int, x: int, y: int):
    if z < 0 or z > 18:
        return Response(status_code=404)

    key = f"{z}/{x}/{y}"
    now = time.time()

    hit = _radar_tile_cache.get(key)
    if hit is not None:
        ts, data = hit
        if (now - ts) < RADAR_TILE_TTL_SECONDS:
            return Response(
                content=data,
                media_type="image/png",
                headers={"Cache-Control": f"public, max-age={RADAR_TILE_TTL_SECONDS}"},
            )

    try:
        png = _fetch_noaa_radar_tile_png(z, x, y, size=256)
    except Exception:
        # Return stale if available; otherwise signal upstream failure
        if hit is not None:
            _, stale = hit
            return Response(content=stale, media_type="image/png", headers={"Cache-Control": "no-store"})
        return Response(status_code=502, content=b"")

    _radar_tile_cache[key] = (now, png)
    return Response(
        content=png,
        media_type="image/png",
        headers={"Cache-Control": f"public, max-age={RADAR_TILE_TTL_SECONDS}"},
    )


@app.get("/api/leaderboard")
def api_leaderboard(
    top: int = Query(25, ge=1, le=200),
    conus: bool = Query(True),
):
    with requests.Session() as session:

        def _fetch():
            return aw_fetch_global_most_recent(session)

        metars = _cached_fetch(_cache_metar, CACHE_SECONDS, _fetch)
        filtered = filter_conus_from_aw(metars) if conus else metars

        now = datetime.now(UTC)
        month = now.month

        rows: List[Dict[str, Any]] = []
        for m in filtered:
            raw = (m.get("rawOb") or "").strip()
            if not raw:
                continue
            rows.append(
                {
                    "product": "METAR",
                    "station": (m.get("icaoId") or "----").strip(),
                    "score": metar_score(raw, model_metar, length_weight=LENGTH_WEIGHT, month=month),
                    "text": raw,
                    "lat": m.get("lat"),
                    "lon": m.get("lon"),
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[: int(top)]

        history.offer_rows("METAR", rows)

        return {"product": "METAR", "top": int(top), "count": len(rows), "rows": rows}


@app.get("/api/taf")
def api_taf(top: int = Query(25, ge=1, le=200)):
    if model_taf is None:
        return JSONResponse(status_code=503, content={"detail": f"TAF model not loaded: {MODEL_TAF_PATH}"})

    with requests.Session() as session:

        def _fetch():
            return aw_fetch_taf_most_recent_global(session)

        tafs = _cached_fetch(_cache_taf, TAF_CACHE_SECONDS, _fetch)

        now = datetime.now(UTC)
        month = now.month

        rows: List[Dict[str, Any]] = []
        for t in tafs:
            raw = (t.get("rawTAF") or t.get("rawOb") or t.get("raw") or "").strip()
            if not raw:
                continue

            station = (t.get("stationId") or t.get("icaoId") or t.get("station") or "----").strip()

            rows.append(
                {
                    "product": "TAF",
                    "station": station,
                    "score": taf_score(raw, model_taf, length_weight=TAF_LENGTH_WEIGHT, month=month),
                    "text": raw,
                    "lat": t.get("lat"),
                    "lon": t.get("lon"),
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[: int(top)]

        history.offer_rows("TAF", rows)

        return {"product": "TAF", "top": int(top), "count": len(rows), "rows": rows}


@app.get("/api/pirep")
def api_pirep(
    top: int = Query(25, ge=1, le=200),
    hours: int = Query(24, ge=1, le=72),
):
    if model_pirep is None:
        return JSONResponse(status_code=503, content={"detail": f"PIREP model not loaded: {MODEL_PIREP_PATH}"})

    with requests.Session() as session:

        def _fetch():
            return aw_fetch_pirep_last_hours_global(session, hours=int(hours))

        now_ts = time.time()
        if (
            _cache_pirep.get("data") is not None
            and _cache_pirep.get("hours") == int(hours)
            and (now_ts - float(_cache_pirep.get("ts", 0.0))) < PIREP_CACHE_SECONDS
        ):
            pireps = _cache_pirep["data"]
        else:
            pireps = _fetch()
            _cache_pirep["ts"] = now_ts
            _cache_pirep["data"] = pireps
            _cache_pirep["hours"] = int(hours)

        now = datetime.now(UTC)
        month = now.month

        rows: List[Dict[str, Any]] = []
        for p in pireps:
            text = (p.get("raw") or p.get("report") or p.get("text") or p.get("rawOb") or "").strip()
            if not text:
                continue
            rows.append(
                {
                    "product": "PIREP",
                    "station": "PIREP",
                    "score": pirep_score(text, model_pirep, length_weight=PIREP_LENGTH_WEIGHT, month=month),
                    "text": text,
                    "lat": p.get("lat"),
                    "lon": p.get("lon"),
                }
            )

        rows.sort(key=lambda x: x["score"], reverse=True)
        rows = rows[: int(top)]

        history.offer_rows("PIREP", rows)

        return {"product": "PIREP", "top": int(top), "hours": int(hours), "count": len(rows), "rows": rows}


# ----------------------------
# Static frontend serving (optional for deployments)
# ----------------------------
FRONTEND_DIST = (BASE_DIR.parent / "frontend" / "dist").resolve()
if FRONTEND_DIST.exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="assets")


@app.get("/")
def root():
    index = FRONTEND_DIST / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse({"ok": True, "detail": "Frontend not built."})


# ⚠️ Catch-all must be LAST so it doesn't shadow /api routes like radar tiles
@app.get("/{full_path:path}")
def spa_fallback(full_path: str, request: Request):
    index = FRONTEND_DIST / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return JSONResponse(status_code=404, content={"detail": "Not found"})
