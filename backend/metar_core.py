#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
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

# AviationWeather endpoints
AW_METAR_URL = "https://aviationweather.gov/api/data/metar"
AW_TAF_URL = "https://aviationweather.gov/api/data/taf"
AW_PIREP_URL = "https://aviationweather.gov/api/data/pirep"
AW_GLOBAL_BBOX = "-180,-90,180,90"

# -----------------------------
# Tokenization + difficulty weighting (METAR)
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
    "CLOUD": 0.2,
    "OTHER": 1.0,
}

LOW_CEILING_BONUS = 0.8


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
    if cls == "WX": return tok

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


# -----------------------------
# Generic seasonal rarity model
# -----------------------------
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

        p = (c_eff + self.alpha) / (t_eff + self.alpha * self.vocab_all) if (t_eff > 0) else \
            (self.counts_all.get(tok, 0) + self.alpha) / (self.total_all + self.alpha * self.vocab_all)

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


# -----------------------------
# Scores for each product
# -----------------------------
def metar_score(raw: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = tokenize_metar(raw)
    score = 0.0
    for tok in set(toks):
        nt = normalize_token(tok)
        score += model.token_rarity(nt, month=month) * token_difficulty(tok)
    score += length_weight * len(raw)
    return score


# For TAF/PIREP: simpler tokenization.
RE_SPLIT = re.compile(r"\s+")


def _simple_tokens(text: str) -> List[str]:
    t = (text or "").strip()
    if not t:
        return []
    return [x for x in RE_SPLIT.split(t) if x]


def taf_score(raw: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = _simple_tokens(raw)
    score = 0.0
    for tok in set(toks):
        score += model.token_rarity(tok, month=month) * 1.0
    score += length_weight * len(raw)
    return score


def pirep_score(text: str, model: SeasonalRarityModel, length_weight: float, month: int) -> float:
    toks = _simple_tokens(text)
    score = 0.0
    for tok in set(toks):
        score += model.token_rarity(tok, month=month) * 1.0
    score += length_weight * len(text)
    return score


# -----------------------------
# CONUS checks
# -----------------------------
def is_in_conus(lat: float, lon: float) -> bool:
    return (CONUS_MIN_LAT <= lat <= CONUS_MAX_LAT) and (CONUS_MIN_LON <= lon <= CONUS_MAX_LON)


# -----------------------------
# PIREP "pilot report only" detector
# -----------------------------
# Classic PIREP format usually includes slash groups like:
#   "... UA /OV ... /TM ... /FL ... /TP ... /TB ... /IC ... /RM ..."
#
# Oceanic "ARP ..." position reports typically do NOT contain "/OV" etc.
PILOT_PIREP_MARKERS = (
    "/OV", "/TM", "/FL", "/TP", "/TB", "/IC", "/WX", "/SK", "/RM", "/FV", "/TA"
)

RE_HAS_PILOT_GROUP = re.compile(r"(^|\s)/(OV|TM|FL|TP|TB|IC|WX|SK|RM|FV|TA)\b")


def is_pilot_pirep(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False

    # Strong positive: contains any standard slash group
    if RE_HAS_PILOT_GROUP.search(t):
        return True

    # Many pilot reports start with "UA" or "UUA" (sometimes appears as "... UA ...")
    # But "UA" alone is too broad, so require at least one slash as well.
    if (t.startswith("UA ") or t.startswith("UUA ") or " UA " in t or " UUA " in t) and "/" in t:
        return True

    return False


# -----------------------------
# AviationWeather fetchers
# -----------------------------
def aw_fetch_global_most_recent(session: requests.Session) -> List[dict]:
    params = {"format": "json", "bbox": AW_GLOBAL_BBOX, "mostRecent": "true"}
    r = session.get(AW_METAR_URL, params=params, timeout=60)
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


def aw_fetch_taf_most_recent_global(session: requests.Session) -> List[dict]:
    params = {"format": "json", "bbox": AW_GLOBAL_BBOX, "mostRecent": "true"}
    r = session.get(AW_TAF_URL, params=params, timeout=60)
    r.raise_for_status()
    if not r.text.strip():
        return []
    return r.json()


def aw_fetch_pirep_last_hours_global(session: requests.Session, hours: int = 24) -> List[dict]:
    params = {"format": "json", "bbox": AW_GLOBAL_BBOX, "hours": str(int(hours))}
    r = session.get(AW_PIREP_URL, params=params, timeout=60)
    r.raise_for_status()
    if not r.text.strip():
        return []
    return r.json()


# -----------------------------
# Optional CLI (kept minimal)
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="Core scoring + AW fetch helpers")
    ap.add_argument("--health", action="store_true", help="Quick import/test")
    args = ap.parse_args()
    if args.health:
        print("ok")


if __name__ == "__main__":
    main()
