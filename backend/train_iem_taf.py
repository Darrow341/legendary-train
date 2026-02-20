#!/usr/bin/env python3
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, UTC
from typing import Dict, Iterable, List, Optional, Tuple

import requests

# Reuse your existing token normalization and SeasonalRarityModel schema
from metar_core import normalize_token

IEM_TAF_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/taf.py"

# ----------------------------
# Tokenization (TAF v1)
# ----------------------------
def tokenize_taf(raw: str) -> List[str]:
    parts = raw.strip().split()
    if not parts:
        return []
    # Drop "TAF"/"TAFAMD"/etc if present
    if parts[0].startswith("TAF"):
        parts = parts[1:]
    # Common prefix: KXXX 201730Z 2018/2124 ...
    if len(parts) >= 2 and parts[0].isalpha() and parts[1].endswith("Z"):
        parts = parts[2:]
    return [p for p in parts if p]


def month_from_iso(s: str) -> Optional[int]:
    s = (s or "").strip()
    if len(s) >= 7 and s[4] == "-" and s[6] == "-":
        try:
            m = int(s[5:7])
            return m if 1 <= m <= 12 else None
        except Exception:
            return None
    return None


def build_conus_station_list(session: requests.Session, timeout: Tuple[int, int]) -> List[str]:
    # Reuse your existing CONUS station list builder (ASOS GeoJSON)
    from metar_core import build_conus_station_list as _b

    return _b(session, timeout=timeout)


# ----------------------------
# IEM CSV streaming (robust)
# ----------------------------
def stream_tafs(
    session: requests.Session,
    stations: List[str],
    sts: datetime,
    ets: datetime,
    timeout: Tuple[int, int],
    max_retries: int = 6,
) -> Iterable[Tuple[str, int]]:
    """
    Streams IEM TAF CSV and yields (raw_taf, month).

    Endpoint parameters per IEM help: station, sts, ets, fmt=csv, tz=UTC.
    Uses TextIOWrapper to guarantee csv.reader gets strings (not bytes).
    Retries on RemoteDisconnected / transient failures.
    """
    params = [
        ("fmt", "csv"),
        ("tz", "UTC"),
        ("sts", sts.strftime("%Y-%m-%dT%H:%MZ")),
        ("ets", ets.strftime("%Y-%m-%dT%H:%MZ")),
    ]
    for s in stations:
        params.append(("station", s))

    for attempt in range(max_retries):
        try:
            with session.get(IEM_TAF_URL, params=params, timeout=timeout, stream=True) as r:
                r.raise_for_status()

                # Force reliable text decoding for CSV
                r.raw.decode_content = True
                text = io.TextIOWrapper(r.raw, encoding=r.encoding or "utf-8", newline="")
                reader = csv.reader(text)

                try:
                    header = next(reader)
                except StopIteration:
                    return

                header_l = [h.strip().lower() for h in header]
                raw_idx = None
                ts_idx = None

                # Typical columns: "station", "issue", "raw" (varies slightly)
                for i, n in enumerate(header_l):
                    if n in ("raw", "raw_taf", "taf", "product"):
                        raw_idx = i
                    if n in ("issue", "issue_time", "issued", "issued_at", "valid", "timestamp", "time"):
                        ts_idx = i

                if raw_idx is None:
                    print(f"[TAF] could not find raw column in header: {header}", file=sys.stderr)
                    return

                default_month = sts.month

                for cols in reader:
                    if not cols or raw_idx >= len(cols):
                        continue

                    raw = (cols[raw_idx] or "").strip()
                    if not raw:
                        continue

                    month = default_month
                    if ts_idx is not None and ts_idx < len(cols):
                        m = month_from_iso(cols[ts_idx])
                        if m:
                            month = m

                    yield raw, month

                return

        except Exception as e:
            sleep_s = min(60, (2 ** attempt))
            print(f"[TAF] request failed (attempt {attempt+1}/{max_retries}): {e} -> sleep {sleep_s}s", file=sys.stderr)
            time.sleep(sleep_s)

    raise RuntimeError("[TAF] request repeatedly failed after retries")


# ----------------------------
# Training
# ----------------------------
def train(
    out_path: str,
    years: int = 5,
    alpha: float = 0.5,
    connect_timeout: int = 20,
    read_timeout: int = 180,
    station_chunk_size: int = 25,
    window_days: int = 7,
) -> None:
    """
    Train a season-aware rarity model from IEM TAF archive.

    - Chunks stations and time windows to avoid oversized responses / disconnects
    - Uses month buckets derived from each row's timestamp when available
    """
    now = datetime.now(UTC)
    ets = now
    sts = now - timedelta(days=365 * years)
    timeout = (connect_timeout, read_timeout)

    counts_all: Dict[str, int] = defaultdict(int)
    total_all = 0
    totals_by_month: Dict[str, int] = {str(m): 0 for m in range(1, 13)}
    counts_by_month: Dict[str, Dict[str, int]] = {str(m): defaultdict(int) for m in range(1, 13)}

    with requests.Session() as session:
        print("Building CONUS station list (ASOS-based)...", file=sys.stderr)
        stations = build_conus_station_list(session, timeout=timeout)
        if not stations:
            raise RuntimeError("No stations found for CONUS list")

        print(
            f"Stations: {len(stations)}  Window: {sts:%Y-%m-%d} → {ets:%Y-%m-%d} (~{years}y) "
            f"| station_chunk_size={station_chunk_size} window_days={window_days}",
            file=sys.stderr,
        )

        t0 = time.time()
        cur = sts
        window = timedelta(days=window_days)

        while cur < ets:
            nxt = min(ets, cur + window)
            print(f"[TAF] {cur:%Y-%m-%d} → {nxt:%Y-%m-%d}", file=sys.stderr)

            for i in range(0, len(stations), station_chunk_size):
                chunk = stations[i : i + station_chunk_size]
                try:
                    for raw, month in stream_tafs(session, chunk, sts=cur, ets=nxt, timeout=timeout):
                        toks = tokenize_taf(raw)
                        norm = {normalize_token(tok) for tok in toks}
                        mkey = str(month)

                        for nt in norm:
                            counts_all[nt] += 1
                            total_all += 1
                            counts_by_month[mkey][nt] += 1
                            totals_by_month[mkey] += 1
                except Exception as e:
                    print(f"[TAF] chunk stations {i}-{i+len(chunk)} failed: {e}", file=sys.stderr)

            cur = nxt

        dt = time.time() - t0
        vocab = len(counts_all)

        model_obj = {
            "trained_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_start_utc": sts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "window_end_utc": ets.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "alpha": alpha,
            "total_all": int(total_all),
            "vocab_all": int(vocab),
            "counts_all": dict(counts_all),
            "totals_by_month": {k: int(v) for k, v in totals_by_month.items()},
            "counts_by_month": {k: dict(v) for k, v in counts_by_month.items()},
            "note": "TAF rarity model trained from IEM /cgi-bin/request/taf.py issuance timestamps.",
        }

        if out_path.endswith(".gz"):
            with gzip.open(out_path, "wt", encoding="utf-8") as f:
                json.dump(model_obj, f)
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(model_obj, f)

        print(
            f"Saved {out_path}  vocab={vocab:,} token_events={total_all:,}  ({dt/60:.1f} min)",
            file=sys.stderr,
        )


if __name__ == "__main__":
    out = os.getenv("OUT", "rarity_taf.json.gz")
    years = int(os.getenv("YEARS", "5"))
    alpha = float(os.getenv("ALPHA", "0.5"))
    station_chunk_size = int(os.getenv("STATION_CHUNK_SIZE", "25"))
    window_days = int(os.getenv("WINDOW_DAYS", "7"))
    connect_timeout = int(os.getenv("CONNECT_TIMEOUT", "20"))
    read_timeout = int(os.getenv("READ_TIMEOUT", "180"))

    train(
        out_path=out,
        years=years,
        alpha=alpha,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        station_chunk_size=station_chunk_size,
        window_days=window_days,
    )
