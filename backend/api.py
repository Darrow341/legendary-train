from __future__ import annotations

import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from metar_core import (
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

# Radar proxy tuning
RADAR_TILE_TTL_SECONDS = int(os.getenv("RADAR_TILE_TTL_SECONDS", "30"))

# NOAA/NWS MRMS base reflectivity MapServer (ArcGIS REST)
NOAA_RADAR_MAPSERVER = os.getenv(
    "NOAA_RADAR_MAPSERVER",
    "https://mapservices.weather.noaa.gov/eventdriven/rest/services/radar/radar_base_reflectivity/MapServer",
)

app = FastAPI(title="Aviation Weather Leaderboard API", version="0.4.0")

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
_cache_taf: Dict[str, Any] = {"ts": 0.0, "data": None}
_cache_pirep: Dict[str, Any] = {"ts": 0.0, "data": None, "hours": None}

# Radar tile cache: key -> (ts, bytes)
_radar_tile_cache: Dict[str, Tuple[float, bytes]] = {}


def _cached_fetch(cache: Dict[str, Any], ttl: int, fetch_fn):
    now = time.time()
    if cache.get("data") is not None and (now - float(cache.get("ts", 0.0))) < ttl:
        return cache["data"]
    data = fetch_fn()
    cache["ts"] = now
    cache["data"] = data
    return data


# ----------------------------
# Radar tile helpers
# ----------------------------
_WEBMERCATOR_R = 6378137.0
_WEBMERCATOR_MAX = 20037508.342789244


def _tile_to_bbox_3857(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    """
    Convert slippy tile (z/x/y) to WebMercator bbox in EPSG:3857 meters.
    """
    n = 2**z

    # tile bounds in "normalized mercator" [0..1]
    x0 = x / n
    x1 = (x + 1) / n
    y0 = y / n
    y1 = (y + 1) / n

    # Convert to lon/lat
    lon_left = x0 * 360.0 - 180.0
    lon_right = x1 * 360.0 - 180.0

    def inv_mercator_lat(t: float) -> float:
        # t in [0..1], y increases downward
        # formula: lat = atan(sinh(pi*(1 - 2t)))
        return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * t))))

    lat_top = inv_mercator_lat(y0)
    lat_bottom = inv_mercator_lat(y1)

    def lonlat_to_3857(lon: float, lat: float) -> Tuple[float, float]:
        lon_rad = math.radians(lon)
        lat_rad = math.radians(max(min(lat, 89.999999), -89.999999))
        mx = _WEBMERCATOR_R * lon_rad
        my = _WEBMERCATOR_R * math.log(math.tan(math.pi / 4.0 + lat_rad / 2.0))
        # Clamp
        mx = max(min(mx, _WEBMERCATOR_MAX), -_WEBMERCATOR_MAX)
        my = max(min(my, _WEBMERCATOR_MAX), -_WEBMERCATOR_MAX)
        return mx, my

    xmin, ymax = lonlat_to_3857(lon_left, lat_top)
    xmax, ymin = lonlat_to_3857(lon_right, lat_bottom)
    return xmin, ymin, xmax, ymax


def _fetch_noaa_radar_tile_png(z: int, x: int, y: int, size: int = 256) -> bytes:
    """
    Call NOAA MapServer /export to get a PNG for this tile bbox.
    """
    xmin, ymin, xmax, ymax = _tile_to_bbox_3857(z, x, y)
    bbox = f"{xmin},{ymin},{xmax},{ymax}"

    export_url = f"{NOAA_RADAR_MAPSERVER}/export"

    # We explicitly request layer 3 (Raster Layer) which contains the reflectivity imagery
    params = {
        "f": "image",
        "bbox": bbox,
        "bboxSR": "3857",
        "imageSR": "3857",
        "size": f"{size},{size}",
        "format": "png32",
        "transparent": "true",
        "layers": "show:3",
        # Cache-buster so upstream/proxies don't pin us
        "_ts": str(int(time.time() * 1000)),
    }

    headers = {
        # These help with some CDNs/hotlink protections
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
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
def health():
    return {
        "ok": True,
        "models": {
            "metar": str(Path(MODEL_METAR_PATH).resolve()),
            "taf": str(Path(MODEL_TAF_PATH).resolve()) if model_taf else None,
            "pirep": str(Path(MODEL_PIREP_PATH).resolve()) if model_pirep else None,
        },
        "radar_mapserver": NOAA_RADAR_MAPSERVER,
    }


@app.get("/api/leaderboard")
def leaderboard(
    top: int = Query(25, ge=1, le=200),
    conus: bool = Query(True),
):
    """
    METAR leaderboard (AviationWeather mostRecent; CONUS bbox in metar_core).
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

            try:
                latf = float(m.get("lat")) if m.get("lat") is not None else None
                lonf = float(m.get("lon")) if m.get("lon") is not None else None
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
):
    """
    TAF leaderboard (AviationWeather mostRecent).
    """
    if model_taf is None:
        return JSONResponse(
            status_code=503,
            content={"detail": f"TAF model not loaded. Missing file: {MODEL_TAF_PATH}"},
        )

    with requests.Session() as session:

        def _fetch():
            return aw_fetch_taf_most_recent_global(session)

        tafs = _cached_fetch(_cache_taf, TAF_CACHE_SECONDS, _fetch)

        now = datetime.now(UTC)
        month = now.month

        rows = []
        for t in tafs:
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

            try:
                latf = float(p.get("lat")) if p.get("lat") is not None else None
                lonf = float(p.get("lon")) if p.get("lon") is not None else None
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


@app.get("/api/radar/tiles/{z}/{x}/{y}.png")
def radar_tile(z: int, x: int, y: int):
    """
    Proxy NOAA/NWS MRMS base reflectivity as standard XYZ tiles.

    This avoids browser-side issues with ArcGIS export + arcgisoutput images
    (HTTP2/caching/hotlink quirks), by fetching server-side and returning PNG.
    """
    # Basic sanity bounds: Leaflet typically uses z 0..18
    if z < 0 or z > 18:
        return Response(status_code=404)

    # Cache key
    key = f"{z}/{x}/{y}"
    now = time.time()

    # Return cached tile if fresh
    hit = _radar_tile_cache.get(key)
    if hit is not None:
        ts, data = hit
        if (now - ts) < RADAR_TILE_TTL_SECONDS:
            return Response(
                content=data,
                media_type="image/png",
                headers={
                    "Cache-Control": f"public, max-age={RADAR_TILE_TTL_SECONDS}",
                },
            )

    try:
        png = _fetch_noaa_radar_tile_png(z, x, y, size=256)
    except Exception:
        # If NOAA errors and we have a stale cached copy, serve it rather than blank
        if hit is not None:
            _, stale = hit
            return Response(content=stale, media_type="image/png", headers={"Cache-Control": "no-store"})
        return Response(status_code=502, content=b"")

    _radar_tile_cache[key] = (now, png)
    return Response(
        content=png,
        media_type="image/png",
        headers={
            "Cache-Control": f"public, max-age={RADAR_TILE_TTL_SECONDS}",
        },
    )


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
