#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Dict, Iterable, List, Tuple, Optional

import requests
from tabulate import tabulate

# -----------------------------
# Geography: CONUS bounds
# -----------------------------
CONUS_MIN_LAT = 24.0
CONUS_MAX_LAT = 50.0
CONUS_MIN_LON = -125.0
CONUS_MAX_LON = -66.0

AW_API_URL = "https://aviationweather.gov/api/data/metar"
AW_GLOBAL_BBOX = "-180,-90,180,90"

IEM_ASOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
IEM_GEOJSON_NETWORK_URL = "https://mesonet.agron.iastate.edu/geojson/network.py"

CONUS_STATE_ABBRS = [
    "AL","AR","AZ","CA","CO","CT","DC","DE","FL","GA","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME",
    "MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD",
    "TN","TX","UT","VA","VT","WA","WI","WV","WY"
]

MAX_STATION_YEARS = 1000
PROGRESS_EVERY_ROWS = 200_000
PROGRESS_EVERY_METARS = 200_000

# -----------------------------
# Tokenization + difficulty weighting
# -----------------------------
DROP_TOKENS = {"METAR"}  # keep SPECI/RMK

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
RE_SECTOR_VIS = re.compile(r"^\d{4}[A-Z]{1,3}$")
RE_RWY_DESIG = re.compile(r"^RWY\d{2}[LRC]?$")
RE_FROPA = re.compile(r"^PRES(FR|RR)$")

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
    "SECTOR_VIS": 3.5,
    "VAR_WIND_DIR": 2.5,
    "WX": 3.0,
    "RMK_MARKER": 0.5,
    "WIND": 1.2,
    "VIS": 1.2,
    "TEMP_DEW": 0.8,
    "ALTIM": 0.3,
    "CLOUD": 0.2,   # clouds heavily downweighted
    "OTHER": 1.0,
}

LOW_CEILING_BONUS = 0.8  # optional small bump for BKN/OVC <= 500 ft


def classify_token(tok: str) -> str:
    if RE_RVR.match(tok): return "RVR"
    if RE_VV.match(tok): return "VV"
    if RE_WIND_SHEAR.match(tok): return "WIND_SHEAR"
    if RE_RUNWAY_STATE.match(tok): return "RUNWAY_STATE"
    if RE_SECTOR_VIS.match(tok): return "SECTOR_VIS"
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
    if cls == "SECTOR_VIS": return "SECTOR_VIS"
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
    if cls == "WX": return tok  # keep exact weather strings

    if RE_RWY_DESIG.match(tok): return "RWY_DESIG"
    if RE_FROPA.match(tok): return "PRES_TREND"
    if tok.startswith("SLP") and len(tok) == 6 and tok[3:].isdigit(): return "SLP"
    if re.match(r"^\d{3,5}/\d{4}$", tok): return "WIND_TIME"
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
                h = int(tok[3:6])  # hundreds of feet
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


# -----------------------------
# Season-aware rarity model
# -----------------------------
@dataclass
class SeasonalRarityModel:
    alpha: float
    total_all: int
    vocab_all: int
    counts_all: Dict[str, int]
    totals_by_month: Dict[str, int]              # "1".."12"
    counts_by_month: Dict[str, Dict[str, int]]   # month -> token -> count

    def token_rarity(self, tok: str, month: int, neighbor_smooth: float = 0.25) -> float:
        """
        Season-aware rarity: -log P(tok | month) with:
        - Laplace smoothing (alpha)
        - Optional neighbor-month smoothing to reduce sparsity:
            effective_count = (1-s)*c_m + (s/2)*(c_prev + c_next)
            effective_total = (1-s)*T_m + (s/2)*(T_prev + T_next)
        - Fallback to global if month data missing.
        """
        m = str(month)
        if m not in self.totals_by_month or m not in self.counts_by_month:
            # fallback global
            c = self.counts_all.get(tok, 0)
            p = (c + self.alpha) / (self.total_all + self.alpha * self.vocab_all)
            return -math.log(p)

        def mprev(mm: int) -> str:
            return str(12 if mm == 1 else mm - 1)

        def mnext(mm: int) -> str:
            return str(1 if mm == 12 else mm + 1)

        mp = mprev(month)
        mn = mnext(month)

        c_m = self.counts_by_month[m].get(tok, 0)
        t_m = self.totals_by_month.get(m, 0)

        if neighbor_smooth > 0:
            c_p = self.counts_by_month.get(mp, {}).get(tok, 0)
            c_n = self.counts_by_month.get(mn, {}).get(tok, 0)
            t_p = self.totals_by_month.get(mp, 0)
            t_n = self.totals_by_month.get(mn, 0)

            s = max(0.0, min(1.0, neighbor_smooth))
            c_eff = (1.0 - s) * c_m + (s / 2.0) * (c_p + c_n)
            t_eff = (1.0 - s) * t_m + (s / 2.0) * (t_p + t_n)
        else:
            c_eff = float(c_m)
            t_eff = float(t_m)

        # Use global vocab for stability across months
        p = (c_eff + self.alpha) / (t_eff + self.alpha * self.vocab_all) if (t_eff > 0) else \
            (self.counts_all.get(tok, 0) + self.alpha) / (self.total_all + self.alpha * self.vocab_all)

        return -math.log(p)


def metar_score(raw: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = tokenize_metar(raw)
    score = 0.0
    for tok in set(toks):
        nt = normalize_token(tok)
        score += model.token_rarity(nt, month=month) * token_difficulty(tok)
    score += length_weight * len(raw)
    return score


# -----------------------------
# CONUS checks
# -----------------------------
def is_in_conus(lat: float, lon: float) -> bool:
    return (CONUS_MIN_LAT <= lat <= CONUS_MAX_LAT) and (CONUS_MIN_LON <= lon <= CONUS_MAX_LON)


# -----------------------------
# Station list via GeoJSON
# -----------------------------
def fetch_geojson_network(session: requests.Session, network: str, timeout: Tuple[int, int]) -> dict:
    r = session.get(IEM_GEOJSON_NETWORK_URL, params={"network": network}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def parse_station_ids_from_geojson(geojson_obj: dict) -> List[Tuple[str, float, float]]:
    feats = geojson_obj.get("features") or []
    out: List[Tuple[str, float, float]] = []
    for f in feats:
        props = f.get("properties") or {}
        sid = (props.get("id") or props.get("sid") or props.get("station") or "").strip()
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") or None
        if not sid or not coords or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        try:
            latf = float(lat)
            lonf = float(lon)
        except Exception:
            continue
        out.append((sid, latf, lonf))
    return out


def build_conus_station_list(session: requests.Session, timeout: Tuple[int, int]) -> List[str]:
    stations: Dict[str, Tuple[float, float]] = {}
    for abbr in CONUS_STATE_ABBRS:
        net = f"{abbr}_ASOS"
        try:
            js = fetch_geojson_network(session, net, timeout=timeout)
        except Exception as e:
            print(f"[stationlist] warning: {net} fetch failed: {e}", file=sys.stderr)
            continue
        for sid, lat, lon in parse_station_ids_from_geojson(js):
            if is_in_conus(lat, lon):
                stations[sid] = (lat, lon)
    return sorted(stations.keys())


def chunk_stations(stations: List[str], years: float, max_station_years: int = MAX_STATION_YEARS) -> List[List[str]]:
    max_per_chunk = max(1, int(max_station_years // years))
    return [stations[i:i + max_per_chunk] for i in range(0, len(stations), max_per_chunk)]


# -----------------------------
# IEM streaming downloader: yields (metar, month)
# -----------------------------
def _month_from_valid(valid_str: str) -> Optional[int]:
    """
    IEM 'valid' usually looks like: 2026-02-19 16:53
    We'll parse safely.
    """
    s = (valid_str or "").strip()
    if not s:
        return None
    # Try common IEM formats
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.month
        except Exception:
            continue
    # Last resort: take month from YYYY-MM-...
    if len(s) >= 7 and s[4] == "-" and s[7-1] == "-":
        try:
            return int(s[5:7])
        except Exception:
            return None
    return None


def iem_stream_metars_for_stations(
    session: requests.Session,
    stations: List[str],
    sts: datetime,
    ets: datetime,
    timeout: Tuple[int, int],
    max_retries: int = 7,
) -> Iterable[Tuple[str, int]]:
    """
    Stream IEM CSV response line-by-line, yielding (raw_metar, month).
    Requires both 'metar' and 'valid' columns.
    """
    params = [
        ("data", "metar"),
        ("format", "onlycomma"),
        ("tz", "UTC"),
        ("sts", sts.strftime("%Y-%m-%dT%H:%M:%SZ")),
        ("ets", ets.strftime("%Y-%m-%dT%H:%M:%SZ")),
    ]
    for s in stations:
        params.append(("station", s))

    for attempt in range(max_retries):
        try:
            with session.get(IEM_ASOS_URL, params=params, timeout=timeout, stream=True) as r:
                if r.status_code in (502, 503, 504):
                    raise requests.HTTPError(f"Server busy: {r.status_code}", response=r)
                r.raise_for_status()

                line_iter = r.iter_lines(decode_unicode=True)

                try:
                    header = next(line_iter)
                except StopIteration:
                    return

                header = header.strip()
                if not header:
                    return

                fieldnames = next(csv.reader([header]))
                metar_idx = None
                valid_idx = None
                for i, name in enumerate(fieldnames):
                    if name == "metar":
                        metar_idx = i
                    elif name == "valid":
                        valid_idx = i

                if metar_idx is None or valid_idx is None:
                    # Unexpected output; abort this chunk
                    return

                rows = 0
                t0 = time.time()

                for line in line_iter:
                    if not line:
                        continue
                    rows += 1
                    cols = next(csv.reader([line]))
                    if metar_idx >= len(cols) or valid_idx >= len(cols):
                        continue

                    met = (cols[metar_idx] or "").strip()
                    if not met:
                        continue

                    month = _month_from_valid(cols[valid_idx]) or 0
                    if 1 <= month <= 12:
                        yield met, month

                    if rows % PROGRESS_EVERY_ROWS == 0:
                        dt = time.time() - t0
                        rate = rows / dt if dt > 0 else 0.0
                        print(f"[IEM] streamed {rows:,} rows ({rate:,.0f} rows/s) ...", file=sys.stderr)

                return

        except Exception as e:
            sleep_s = min(120, (2 ** attempt) + random.random())
            print(f"[IEM] attempt {attempt+1}/{max_retries} failed: {e} -> sleeping {sleep_s:.1f}s", file=sys.stderr)
            time.sleep(sleep_s)

    raise RuntimeError("IEM request repeatedly failed; try later or reduce request size.")


# -----------------------------
# Training: builds season-aware rarity model
# -----------------------------
def train_rarity_model(
    out_path: str,
    years: int = 2,
    alpha: float = 0.5,
    connect_timeout: int = 20,
    read_timeout: int = 120,
    checkpoint_path: str | None = "rarity_checkpoint.json",
) -> None:
    now = datetime.now(UTC)
    ets = now
    sts = now - timedelta(days=365 * years)
    timeout = (connect_timeout, read_timeout)

    with requests.Session() as session:
        print("Building CONUS station list from IEM GeoJSON...")
        stations = build_conus_station_list(session, timeout=timeout)
        if not stations:
            raise RuntimeError("Could not build station list (empty). Network/DNS/firewall may block IEM.")

        yrs = (ets - sts).total_seconds() / (365.0 * 24 * 3600)
        chunks = chunk_stations(stations, years=yrs)

        print(f"Stations (CONUS): {len(stations)}")
        print(f"Time window: {sts.strftime('%Y-%m-%d')} to {ets.strftime('%Y-%m-%d')} (~{yrs:.2f} years)")
        print(f"Chunks: {len(chunks)} (<= {MAX_STATION_YEARS} station-years/request)")

        counts_all: Dict[str, int] = {}
        total_all = 0

        totals_by_month: Dict[str, int] = {str(m): 0 for m in range(1, 13)}
        counts_by_month: Dict[str, Dict[str, int]] = {str(m): {} for m in range(1, 13)}

        start_chunk = 1

        # Resume checkpoint if present
        if checkpoint_path and os.path.exists(checkpoint_path):
            try:
                with open(checkpoint_path, "r", encoding="utf-8") as f:
                    ck = json.load(f)
                counts_all = {k: int(v) for k, v in ck["counts_all"].items()}
                total_all = int(ck["total_all"])
                totals_by_month = {k: int(v) for k, v in ck["totals_by_month"].items()}
                counts_by_month = {k: {tk: int(tv) for tk, tv in v.items()} for k, v in ck["counts_by_month"].items()}
                start_chunk = int(ck.get("next_chunk", 1))
                print(f"Resuming from checkpoint: {checkpoint_path} (next chunk {start_chunk})")
            except Exception as e:
                print(f"Warning: could not load checkpoint: {e}", file=sys.stderr)

        for i in range(start_chunk, len(chunks) + 1):
            chunk = chunks[i - 1]
            print(f"\nDownloading chunk {i}/{len(chunks)} with {len(chunk)} stations...")
            n_metars = 0
            t0 = time.time()

            for raw, month in iem_stream_metars_for_stations(session, chunk, sts=sts, ets=ets, timeout=timeout):
                n_metars += 1
                toks = tokenize_metar(raw)
                norm_tokens = {normalize_token(tok) for tok in toks}

                mkey = str(month)

                for nt in norm_tokens:
                    counts_all[nt] = counts_all.get(nt, 0) + 1
                    total_all += 1

                    # month-conditional counts
                    cm = counts_by_month[mkey]
                    cm[nt] = cm.get(nt, 0) + 1
                    totals_by_month[mkey] += 1

                if n_metars % PROGRESS_EVERY_METARS == 0:
                    dt = time.time() - t0
                    rate = n_metars / dt if dt > 0 else 0.0
                    print(f"  processed {n_metars:,} METARs ({rate:,.0f} metars/s) ...", file=sys.stderr)

            dt = time.time() - t0
            rate = n_metars / dt if dt > 0 else 0.0
            print(f"  METAR rows processed: {n_metars:,} ({rate:,.0f} metars/s)")
            print(f"  Vocab so far: {len(counts_all):,} normalized tokens | total token-events: {total_all:,}")

            if checkpoint_path:
                tmp = {
                    "counts_all": counts_all,
                    "total_all": total_all,
                    "totals_by_month": totals_by_month,
                    "counts_by_month": counts_by_month,
                    "next_chunk": i + 1,
                    "updated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                with open(checkpoint_path, "w", encoding="utf-8") as f:
                    json.dump(tmp, f)
                print(f"  Wrote checkpoint: {checkpoint_path}")

        model_obj = {
            "trained_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_start_utc": sts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_end_utc": ets.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "alpha": alpha,
            "total_all": total_all,
            "vocab_all": len(counts_all),
            "counts_all": counts_all,
            "totals_by_month": totals_by_month,
            "counts_by_month": counts_by_month,
            "note": "Season-normalized rarity uses P(token|month) with neighbor-month smoothing.",
        }

        if out_path.endswith(".gz"):
            with gzip.open(out_path, "wt", encoding="utf-8") as f:
                json.dump(model_obj, f)
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(model_obj, f)

        print(f"\nSaved rarity model: {out_path}")
        print(f"Vocab: {len(counts_all):,} normalized tokens | Total token-events: {total_all:,}")

        if checkpoint_path and os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)
            print(f"Removed checkpoint: {checkpoint_path}")


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


# -----------------------------
# Live fetching (AviationWeather)
# -----------------------------
def aw_fetch_global_most_recent(session: requests.Session) -> List[dict]:
    params = {"format": "json", "bbox": AW_GLOBAL_BBOX, "mostRecent": "true"}
    r = session.get(AW_API_URL, params=params, timeout=60)
    r.raise_for_status()
    if not r.text.strip():
        return []
    return r.json()


def filter_conus_from_aw(metars: List[dict]) -> List[dict]:
    out = []
    for m in metars:
        icao = (m.get("icaoId") or "").strip()
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


def render_leaderboard(rows: List[dict], title: str) -> None:
    os.system("clear")
    print(title)
    print(f"UTC: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 140)
    if not rows:
        print("No rows.")
        return
    table = []
    for i, e in enumerate(rows, 1):
        table.append([i, e["station"], f"{e['score']:.2f}", e["metar"]])
    print(tabulate(table, headers=["Rank", "ICAO", "Score", "METAR"], tablefmt="grid"))


def live_loop(model_path: str, length_weight: float, poll_interval: int, top_n: int) -> None:
    model = load_model(model_path)

    with requests.Session() as session:
        while True:
            try:
                now = datetime.now(UTC)
                month = now.month

                metars = aw_fetch_global_most_recent(session)
                conus = filter_conus_from_aw(metars)

                scored = []
                for m in conus:
                    raw = (m.get("rawOb") or "").strip()
                    if not raw:
                        continue
                    scored.append({
                        "station": m.get("icaoId", "----"),
                        "score": metar_score(raw, model, length_weight=length_weight, month=month),
                        "metar": raw,
                    })

                scored.sort(key=lambda x: x["score"], reverse=True)
                render_leaderboard(
                    scored[:top_n],
                    "🇺🇸 HARD-TO-DECODE METAR LEADERBOARD (CONUS) — season-normalized rarity * difficulty + length"
                )

            except KeyboardInterrupt:
                print("\nShutting down.")
                return
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)

            time.sleep(poll_interval)


# -----------------------------
# CLI
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Season-normalized hard-to-decode METAR leaderboard (IEM train + live)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_train = sub.add_parser("train", help="Train season-aware rarity model from prior N years of IEM METARs (CONUS)")
    ap_train.add_argument("--years", type=int, default=2)
    ap_train.add_argument("--out", type=str, default="rarity_model.json")
    ap_train.add_argument("--alpha", type=float, default=0.5)
    ap_train.add_argument("--connect-timeout", type=int, default=20)
    ap_train.add_argument("--read-timeout", type=int, default=120)
    ap_train.add_argument("--checkpoint", type=str, default="rarity_checkpoint.json")

    ap_live = sub.add_parser("live", help="Run live leaderboard using a trained season-aware rarity model")
    ap_live.add_argument("--model", type=str, default="rarity_model.json")
    ap_live.add_argument("--length-weight", type=float, default=0.02)
    ap_live.add_argument("--poll", type=int, default=900)
    ap_live.add_argument("--top", type=int, default=25)

    args = ap.parse_args()

    if args.cmd == "train":
        train_rarity_model(
            out_path=args.out,
            years=args.years,
            alpha=args.alpha,
            connect_timeout=args.connect_timeout,
            read_timeout=args.read_timeout,
            checkpoint_path=args.checkpoint or None,
        )
    elif args.cmd == "live":
        live_loop(
            model_path=args.model,
            length_weight=args.length_weight,
            poll_interval=args.poll,
            top_n=args.top,
        )
