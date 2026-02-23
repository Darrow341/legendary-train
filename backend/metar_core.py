#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Dict, List, Optional, Tuple

import requests

CONUS_MIN_LAT = 24.0
CONUS_MAX_LAT = 50.0
CONUS_MIN_LON = -125.0
CONUS_MAX_LON = -66.0


def is_in_conus(lat: float, lon: float) -> bool:
    return (CONUS_MIN_LAT <= lat <= CONUS_MAX_LAT) and (CONUS_MIN_LON <= lon <= CONUS_MAX_LON)


AW_METAR_URL = "https://aviationweather.gov/api/data/metar"
AW_TAF_URL = "https://aviationweather.gov/api/data/taf"
AW_PIREP_URL = "https://aviationweather.gov/api/data/pirep"

AW_METAR_CACHE_XML_GZ = "https://aviationweather.gov/data/cache/metars.cache.xml.gz"
AW_TAF_CACHE_XML_GZ = "https://aviationweather.gov/data/cache/tafs.cache.xml.gz"
AW_AIRCRAFTREP_XML_GZ = "https://aviationweather.gov/data/cache/aircraftreports.cache.xml.gz"
AW_STATIONS_CACHE_JSON_GZ = "https://aviationweather.gov/data/cache/stations.cache.json.gz"

AW_METAR_CACHE_XML_GZ_WWW = "https://www.aviationweather.gov/data/cache/metars.cache.xml.gz"
AW_STATIONS_CACHE_JSON_GZ_WWW = "https://www.aviationweather.gov/data/cache/stations.cache.json.gz"

AW_CONUS_BBOX = f"{CONUS_MIN_LON},{CONUS_MIN_LAT},{CONUS_MAX_LON},{CONUS_MAX_LAT}"

AW_HEADERS = {
    "User-Agent": "metar-webapp/1.0 (local; contact=local)",
    "Accept": "application/json, text/plain, */*",
}
AW_TIMEOUT = 60

DEBUG = os.environ.get("METAR_DEBUG", "").strip() not in ("", "0", "false", "False")


def _dbg(msg: str) -> None:
    if DEBUG:
        print(f"[metar_core] {msg}", file=sys.stderr)


# Tune chunk size without editing code:
#   set -x METAR_IDS_CHUNK_SIZE 100
DEFAULT_IDS_CHUNK_SIZE = int(os.environ.get("METAR_IDS_CHUNK_SIZE", "100"))
DEFAULT_IDS_CHUNK_SIZE = max(10, min(250, DEFAULT_IDS_CHUNK_SIZE))

# Optional: request recent window too (harmless if ignored by upstream)
DEFAULT_METAR_HOURS = os.environ.get("METAR_HOURS", "").strip()
DEFAULT_METAR_HOURS = DEFAULT_METAR_HOURS if DEFAULT_METAR_HOURS.isdigit() else ""


_LAST_METAR_FETCH_DEBUG: Dict[str, object] = {}


def get_last_metar_fetch_debug() -> Dict[str, object]:
    return dict(_LAST_METAR_FETCH_DEBUG)


def _set_fetch_debug(**kwargs) -> None:
    _LAST_METAR_FETCH_DEBUG.clear()
    _LAST_METAR_FETCH_DEBUG.update(kwargs)


def _swap_bbox_string(bbox: str) -> str:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        return bbox
    a, b, c, d = parts
    return f"{b},{a},{d},{c}"


def _aw_get_json_list(
    session: requests.Session,
    url: str,
    params: dict,
    *,
    retry_bbox_swap: bool = True,
) -> List[dict]:
    for k, v in AW_HEADERS.items():
        session.headers.setdefault(k, v)

    def _do(p: dict) -> requests.Response:
        return session.get(url, params=p, timeout=AW_TIMEOUT)

    resp = _do(params)

    if resp.status_code == 204:
        return []

    if retry_bbox_swap and "bbox" in params:
        if resp.status_code == 400:
            swapped = dict(params)
            swapped["bbox"] = _swap_bbox_string(str(params["bbox"]))
            resp2 = _do(swapped)
            if resp2.status_code == 204:
                return []
            resp2.raise_for_status()
            txt2 = (resp2.text or "").strip()
            if not txt2:
                return []
            data2 = resp2.json()
            return data2 if isinstance(data2, list) else []

        resp.raise_for_status()
        txt = (resp.text or "").strip()
        if not txt:
            return []
        data = resp.json()

        if isinstance(data, list) and len(data) == 0:
            swapped = dict(params)
            swapped["bbox"] = _swap_bbox_string(str(params["bbox"]))
            resp2 = _do(swapped)
            if resp2.status_code == 204:
                return []
            resp2.raise_for_status()
            txt2 = (resp2.text or "").strip()
            if not txt2:
                return []
            data2 = resp2.json()
            return data2 if isinstance(data2, list) else []

        return data if isinstance(data, list) else []

    resp.raise_for_status()
    txt = (resp.text or "").strip()
    if not txt:
        return []
    data = resp.json()
    return data if isinstance(data, list) else []


def _http_get_bytes(session: requests.Session, url: str) -> bytes:
    for k, v in AW_HEADERS.items():
        session.headers.setdefault(k, v)
    r = session.get(url, timeout=AW_TIMEOUT)
    r.raise_for_status()
    return r.content


def _gunzip(data: bytes) -> bytes:
    return gzip.decompress(data)


def _looks_like_real_metar_cache(xml_bytes: bytes) -> bool:
    if len(xml_bytes) < 10_000:
        return False
    head = xml_bytes[:500].lower()
    if b"<html" in head:
        return False
    if b"<metar" not in xml_bytes[:200_000].lower():
        return False
    return True


def _parse_metar_cache_xml_from_url(session: requests.Session, url: str) -> List[dict]:
    raw_gz = _http_get_bytes(session, url)
    xml_bytes = _gunzip(raw_gz)

    if not _looks_like_real_metar_cache(xml_bytes):
        _dbg(f"METAR cache invalid from {url}: unzipped_bytes={len(xml_bytes)}")
        return []

    root = ET.fromstring(xml_bytes)
    out: List[dict] = []
    for m in root.iterfind(".//METAR"):
        raw_text = (m.findtext("raw_text") or "").strip()
        station = (m.findtext("station_id") or "").strip()
        lat = m.findtext("latitude")
        lon = m.findtext("longitude")
        obs_time = (m.findtext("observation_time") or "").strip()

        if not raw_text or not station:
            continue

        obj = {"rawOb": raw_text, "icaoId": station}
        try:
            if lat and lon:
                obj["lat"] = float(lat)
                obj["lon"] = float(lon)
        except Exception:
            pass
        if obs_time:
            obj["observation_time"] = obs_time

        out.append(obj)
    return out


def _parse_taf_cache_xml_gz(session: requests.Session) -> List[dict]:
    raw = _gunzip(_http_get_bytes(session, AW_TAF_CACHE_XML_GZ))
    root = ET.fromstring(raw)
    out: List[dict] = []
    for taf in root.iterfind(".//TAF"):
        raw_text = (taf.findtext("raw_text") or "").strip()
        station = (taf.findtext("station_id") or taf.findtext("icao_id") or "").strip()
        lat = taf.findtext("latitude")
        lon = taf.findtext("longitude")
        if not raw_text or not station:
            continue
        obj = {"raw": raw_text, "icaoId": station}
        try:
            if lat and lon:
                obj["lat"] = float(lat)
                obj["lon"] = float(lon)
        except Exception:
            pass
        out.append(obj)
    return out


def _parse_aircraftreports_xml_gz(session: requests.Session, hours: int = 24) -> List[dict]:
    raw = _gunzip(_http_get_bytes(session, AW_AIRCRAFTREP_XML_GZ))
    root = ET.fromstring(raw)
    cutoff = datetime.now(UTC).timestamp() - (int(hours) * 3600)

    out: List[dict] = []
    for rep in root.iterfind(".//AircraftReport"):
        raw_text = (rep.findtext("raw_text") or "").strip()
        if not raw_text:
            continue
        if raw_text.lstrip().startswith("ARP "):
            continue

        lat = rep.findtext("latitude")
        lon = rep.findtext("longitude")
        obs_time = (rep.findtext("observation_time") or rep.findtext("report_time") or "").strip()

        latf = lonf = None
        try:
            if lat and lon:
                latf, lonf = float(lat), float(lon)
        except Exception:
            latf = lonf = None

        if latf is not None and lonf is not None and not is_in_conus(latf, lonf):
            continue

        if obs_time:
            try:
                dt = datetime.fromisoformat(obs_time.replace("Z", "+00:00"))
                if dt.timestamp() < cutoff:
                    continue
            except Exception:
                pass

        obj = {"raw": raw_text}
        if latf is not None and lonf is not None:
            obj["lat"] = latf
            obj["lon"] = lonf
        if obs_time:
            obj["observation_time"] = obs_time

        out.append(obj)

    return out


def _load_conus_k_station_ids(session: requests.Session) -> List[str]:
    urls = [AW_STATIONS_CACHE_JSON_GZ, AW_STATIONS_CACHE_JSON_GZ_WWW]
    data_obj = None
    last_err: Optional[Exception] = None

    for u in urls:
        try:
            raw = _gunzip(_http_get_bytes(session, u))
            if len(raw) < 5_000:
                _dbg(f"Stations cache tiny from {u}: unzipped_bytes={len(raw)}")
                continue
            data_obj = json.loads(raw.decode("utf-8", errors="replace"))
            break
        except Exception as e:
            last_err = e

    if data_obj is None:
        if last_err:
            raise last_err
        raise RuntimeError("Failed to load stations cache")

    stations = None
    if isinstance(data_obj, dict):
        if isinstance(data_obj.get("data"), list):
            stations = data_obj["data"]
        elif isinstance(data_obj.get("Station"), list):
            stations = data_obj["Station"]
        elif isinstance(data_obj.get("stations"), list):
            stations = data_obj["stations"]

    if not isinstance(stations, list):
        stations = []

    ids: List[str] = []
    for st in stations:
        if not isinstance(st, dict):
            continue
        sid = (st.get("station_id") or st.get("icaoId") or st.get("id") or "").strip().upper()
        if not sid or not sid.startswith("K"):
            continue

        lat = st.get("latitude") if st.get("latitude") is not None else st.get("lat")
        lon = st.get("longitude") if st.get("longitude") is not None else st.get("lon")

        try:
            latf = float(lat)
            lonf = float(lon)
        except Exception:
            continue

        if is_in_conus(latf, lonf):
            ids.append(sid)

    seen = set()
    out: List[str] = []
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            out.append(sid)

    _dbg(f"Stations-derived ids: {len(out)}")
    return out


def _fetch_metars_by_ids_chunked(session: requests.Session, ids: List[str], chunk_size: int) -> Tuple[List[dict], int, int]:
    out: List[dict] = []
    ok = 0
    bad = 0

    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]
        params = {"format": "json", "mostRecent": "true", "ids": ",".join(chunk)}
        if DEFAULT_METAR_HOURS:
            params["hours"] = DEFAULT_METAR_HOURS

        try:
            data = _aw_get_json_list(session, AW_METAR_URL, params, retry_bbox_swap=False)
            if data:
                out.extend(data)
            ok += 1
        except Exception as e:
            bad += 1
            _dbg(f"METAR ids chunk failed (size={len(chunk)}): {e!r}")

    return out, ok, bad


def _taf_station_ids_conus_k(tafs: List[dict]) -> List[str]:
    ids: List[str] = []
    for t in tafs:
        sid = (t.get("icaoId") or t.get("station_id") or t.get("stationId") or "").strip().upper()
        if not sid.startswith("K"):
            continue
        lat = t.get("lat")
        lon = t.get("lon")
        if lat is not None and lon is not None:
            try:
                if not is_in_conus(float(lat), float(lon)):
                    continue
            except Exception:
                pass
        ids.append(sid)

    seen = set()
    out: List[str] = []
    for sid in ids:
        if sid not in seen:
            seen.add(sid)
            out.append(sid)
    return out


def aw_fetch_taf_most_recent_global(session: requests.Session) -> List[dict]:
    params = {"format": "json", "bbox": AW_CONUS_BBOX, "mostRecent": "true"}
    try:
        data = _aw_get_json_list(session, AW_TAF_URL, params, retry_bbox_swap=True)
        if data:
            return data
    except Exception as e:
        _dbg(f"TAF API failed: {e!r}")
    try:
        return _parse_taf_cache_xml_gz(session)
    except Exception as e:
        _dbg(f"TAF cache failed: {e!r}")
        return []


def aw_fetch_pirep_last_hours_global(session: requests.Session, hours: int = 24) -> List[dict]:
    params = {"format": "json", "bbox": AW_CONUS_BBOX, "hours": str(int(hours))}
    try:
        data = _aw_get_json_list(session, AW_PIREP_URL, params, retry_bbox_swap=True)
        if data:
            return data
    except Exception as e:
        _dbg(f"PIREP API failed: {e!r}")
    try:
        return _parse_aircraftreports_xml_gz(session, hours=hours)
    except Exception as e:
        _dbg(f"PIREP cache failed: {e!r}")
        return []


def aw_fetch_global_most_recent(session: requests.Session) -> List[dict]:
    start = time.time()
    chunk_size = DEFAULT_IDS_CHUNK_SIZE

    bbox_err = None
    try:
        params = {"format": "json", "bbox": AW_CONUS_BBOX, "mostRecent": "true"}
        data = _aw_get_json_list(session, AW_METAR_URL, params, retry_bbox_swap=True)
        if data:
            _set_fetch_debug(
                strategy="api_bbox",
                fetched=len(data),
                seconds=round(time.time() - start, 3),
                ids_chunk_size=chunk_size,
            )
            return data
    except Exception as e:
        bbox_err = repr(e)
        _dbg(f"METAR bbox failed: {e!r}")

    taf_err = None
    try:
        tafs = aw_fetch_taf_most_recent_global(session)
        ids = _taf_station_ids_conus_k(tafs)
        if ids:
            metars, ok, bad = _fetch_metars_by_ids_chunked(session, ids, chunk_size=chunk_size)
            if metars:
                _set_fetch_debug(
                    strategy="taf_ids",
                    taf_ids=len(ids),
                    chunks_ok=ok,
                    chunks_failed=bad,
                    fetched=len(metars),
                    seconds=round(time.time() - start, 3),
                    bbox_err=bbox_err,
                    ids_chunk_size=chunk_size,
                    metar_hours=DEFAULT_METAR_HOURS or None,
                )
                return metars
    except Exception as e:
        taf_err = repr(e)
        _dbg(f"METAR TAF-derived ids failed: {e!r}")

    stations_err = None
    try:
        ids = _load_conus_k_station_ids(session)
        if ids:
            metars, ok, bad = _fetch_metars_by_ids_chunked(session, ids, chunk_size=chunk_size)
            if metars:
                _set_fetch_debug(
                    strategy="stations_ids",
                    station_ids=len(ids),
                    chunks_ok=ok,
                    chunks_failed=bad,
                    fetched=len(metars),
                    seconds=round(time.time() - start, 3),
                    bbox_err=bbox_err,
                    taf_err=taf_err,
                    ids_chunk_size=chunk_size,
                    metar_hours=DEFAULT_METAR_HOURS or None,
                )
                return metars
    except Exception as e:
        stations_err = repr(e)
        _dbg(f"METAR stations cache ids failed: {e!r}")

    cache1_err = None
    try:
        data = _parse_metar_cache_xml_from_url(session, AW_METAR_CACHE_XML_GZ)
        if data:
            _set_fetch_debug(
                strategy="xml_cache_primary",
                fetched=len(data),
                seconds=round(time.time() - start, 3),
                bbox_err=bbox_err,
                taf_err=taf_err,
                stations_err=stations_err,
                ids_chunk_size=chunk_size,
            )
            return data
    except Exception as e:
        cache1_err = repr(e)
        _dbg(f"METAR cache primary failed: {e!r}")

    cache2_err = None
    try:
        data = _parse_metar_cache_xml_from_url(session, AW_METAR_CACHE_XML_GZ_WWW)
        if data:
            _set_fetch_debug(
                strategy="xml_cache_www",
                fetched=len(data),
                seconds=round(time.time() - start, 3),
                bbox_err=bbox_err,
                taf_err=taf_err,
                stations_err=stations_err,
                cache1_err=cache1_err,
                ids_chunk_size=chunk_size,
            )
            return data
    except Exception as e:
        cache2_err = repr(e)
        _dbg(f"METAR cache www failed: {e!r}")

    _set_fetch_debug(
        strategy="none",
        fetched=0,
        seconds=round(time.time() - start, 3),
        bbox_err=bbox_err,
        taf_err=taf_err,
        stations_err=stations_err,
        cache1_err=cache1_err,
        cache2_err=cache2_err,
        ids_chunk_size=chunk_size,
        metar_hours=DEFAULT_METAR_HOURS or None,
    )
    return []


def filter_conus_from_aw(metars: List[dict]) -> List[dict]:
    out: List[dict] = []
    for m in metars:
        icao = (m.get("icaoId") or "").strip().upper()
        lat = m.get("lat")
        lon = m.get("lon")
        if not icao or lat is None or lon is None:
            continue
        if not icao.startswith("K"):
            continue
        try:
            latf, lonf = float(lat), float(lon)
        except Exception:
            continue
        if is_in_conus(latf, lonf):
            out.append(m)
    return out


# -----------------------------
# Tokenization + scoring (unchanged)
# -----------------------------
DROP_TOKENS = {"METAR"}

RE_RVR = re.compile(r"^R\d{2}[LRC]?/")
RE_VV = re.compile(r"^VV\d{3}$")
RE_WIND_SHEAR = re.compile(r"^WS(RWY\d{2}[LRC]?|\d{3}/\d{2,3}KT)$")
RE_RUNWAY_STATE = re.compile(r"^R\d{2}[LRC]?\d{4}/")
RE_VAR_WIND_DIR = re.compile(r"^\d{3}V\d{3}$")
RE_WIND = re.compile(r"^(VRB|\d{3})\d{2,3}(G\d{2,3})?KT$")
RE_VIS_SM = re.compile(r"^(\d+|\d+/\d+|\d+\s\d+/\d+)SM$")
RE_TEMP_DEW = re.compile(r"^(M?\d{2})/(M?\d{2})$")
RE_ALTIM = re.compile(r"^A\d{4}$|^Q\d{4}$")
RE_CLOUD = re.compile(r"^(FEW|SCT|BKN|OVC)\d{3}(CB|TCU)?$")
RE_WX = re.compile(r"^(\+|-)?(VC)?[A-Z]{2,6}$")

HARD_RMK_KEYWORDS = {
    "TORNADO", "FUNNEL", "WATERSPOUT",
    "HAIL", "GR", "GS", "FC",
    "LTG", "LTGIC", "LTGCG", "LTGCC", "LTGCA",
    "PK", "WND", "WSHFT", "FROPA", "PRESFR", "PRESRR",
    "TWR", "SFC", "VIS",
    "RVR",
    "VIRGA",
    "VA", "VASH",
}

DIFFICULTY_WEIGHT = {
    "RVR": 6.0,
    "VV": 5.0,
    "WIND_SHEAR": 5.0,
    "RUNWAY_STATE": 4.0,
    "VAR_WIND_DIR": 2.5,
    "WX": 3.0,
    "RMK_MARKER": 0.5,
    "WIND": 1.2,
    "VIS": 1.2,
    "TEMP_DEW": 0.8,
    "ALTIM": 0.3,
    "CLOUD": 0.2,
    "OTHER": 1.0,
}

LOW_CEILING_BONUS = 0.8


def classify_token(tok: str) -> str:
    if RE_RVR.match(tok): return "RVR"
    if RE_VV.match(tok): return "VV"
    if RE_WIND_SHEAR.match(tok): return "WIND_SHEAR"
    if RE_RUNWAY_STATE.match(tok): return "RUNWAY_STATE"
    if RE_VAR_WIND_DIR.match(tok): return "VAR_WIND_DIR"
    if tok == "RMK": return "RMK_MARKER"
    if RE_WX.match(tok): return "WX"
    if RE_WIND.match(tok): return "WIND"
    if RE_VIS_SM.match(tok): return "VIS"
    if RE_TEMP_DEW.match(tok): return "TEMP_DEW"
    if RE_ALTIM.match(tok): return "ALTIM"
    if RE_CLOUD.match(tok): return "CLOUD"
    return "OTHER"


def normalize_token(tok: str) -> str:
    cls = classify_token(tok)
    if cls == "ALTIM": return "ALTIMETER"
    if cls == "TEMP_DEW": return "TEMP_DEW"
    if cls == "WIND":
        if tok.startswith("VRB"): return "WIND_VRB"
        if "G" in tok: return "WIND_GUST"
        return "WIND"
    if cls == "VIS": return "VIS"
    if cls == "VAR_WIND_DIR": return "VAR_WIND_DIR"
    if cls == "CLOUD":
        m = RE_CLOUD.match(tok)
        if not m: return "CLOUD"
        base = m.group(1)
        conv = m.group(2)
        return f"CLOUD_{base}_{conv}" if conv else f"CLOUD_{base}"
    if cls == "VV": return "VV"
    if cls == "RVR": return "RVR"
    if cls == "WIND_SHEAR": return "WIND_SHEAR"
    if cls == "RUNWAY_STATE": return "RUNWAY_STATE"
    if cls == "RMK_MARKER": return "RMK"
    if cls == "WX": return tok
    return tok


def token_difficulty(tok: str) -> float:
    cls = classify_token(tok)
    if cls == "OTHER" and tok in HARD_RMK_KEYWORDS:
        return 4.0
    base = DIFFICULTY_WEIGHT.get(cls, 1.0)
    if cls == "CLOUD" and LOW_CEILING_BONUS > 0:
        m = RE_CLOUD.match(tok)
        if m:
            try:
                h = int(tok[3:6])
                if h <= 5 and m.group(1) in ("BKN", "OVC"):
                    base = base + LOW_CEILING_BONUS
            except Exception:
                pass
    return base


def tokenize_metar(raw: str) -> List[str]:
    parts = raw.strip().split()
    if not parts:
        return []
    if parts[0] == "METAR":
        parts = parts[1:]
    if len(parts) >= 2 and parts[0].isalpha() and parts[1].endswith("Z") and parts[1][:-1].isdigit():
        parts = parts[2:]
    return [p for p in parts if p and p not in DROP_TOKENS]


@dataclass
class SeasonalRarityModel:
    alpha: float
    total_all: int
    vocab_all: int
    counts_all: Dict[str, int]
    totals_by_month: Dict[str, int]
    counts_by_month: Dict[str, Dict[str, int]]

    def token_rarity(self, tok: str, month: int, neighbor_smooth: float = 0.25) -> float:
        m = str(month)
        if m not in self.totals_by_month or m not in self.counts_by_month:
            c = self.counts_all.get(tok, 0)
            p = (c + self.alpha) / (self.total_all + self.alpha * self.vocab_all)
            return -math.log(p)
        c_m = self.counts_by_month[m].get(tok, 0)
        t_m = self.totals_by_month.get(m, 0)
        p = (c_m + self.alpha) / (t_m + self.alpha * self.vocab_all) if t_m > 0 else (
            (self.counts_all.get(tok, 0) + self.alpha) / (self.total_all + self.alpha * self.vocab_all)
        )
        return -math.log(p)


def load_model(path: str) -> SeasonalRarityModel:
    if path.endswith(".gz"):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            obj = json.load(f)
    else:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)

    counts_all = {k: int(v) for k, v in obj["counts_all"].items()}
    totals_by_month = {k: int(v) for k, v in obj["totals_by_month"].items()}
    counts_by_month = {k: {tk: int(tv) for tk, tv in v.items()} for k, v in obj["counts_by_month"].items()}

    return SeasonalRarityModel(
        alpha=float(obj.get("alpha", 0.5)),
        total_all=int(obj["total_all"]),
        vocab_all=int(obj["vocab_all"]),
        counts_all=counts_all,
        totals_by_month=totals_by_month,
        counts_by_month=counts_by_month,
    )


RE_SPLIT = re.compile(r"\s+")


def _simple_tokens(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    return [x for x in RE_SPLIT.split(t) if x]


def metar_score(raw: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = tokenize_metar(raw)
    score = 0.0
    for tok in set(toks):
        nt = normalize_token(tok)
        score += model.token_rarity(nt, month=month) * token_difficulty(tok)
    score += length_weight * len(raw)
    return score


def taf_score(raw: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = _simple_tokens(raw)
    score = 0.0
    for tok in set(toks):
        score += model.token_rarity(tok, month=month)
    score += length_weight * len(raw)
    return score


def pirep_score(text: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = _simple_tokens(text)
    score = 0.0
    for tok in set(toks):
        score += model.token_rarity(tok, month=month)
    score += length_weight * len(text)
    return score


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--health", action="store_true")
    args = ap.parse_args()
    if args.health:
        print("ok", datetime.now(UTC).isoformat())


if __name__ == "__main__":
    main()
