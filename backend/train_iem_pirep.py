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

from metar_core import normalize_token

IEM_PIREP_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/pireps.py"

# Allow very large CSV fields (IEM PIREP REPORT can occasionally be huge)
try:
    csv.field_size_limit(10 * 1024 * 1024)  # 10 MiB
except Exception:
    # Some platforms cap this; safe to ignore
    pass


# ----------------------------
# Tokenization (PIREP v1)
# ----------------------------
def tokenize_pirep(raw: str) -> List[str]:
    return [p for p in raw.strip().split() if p]


def month_from_iso(s: str) -> Optional[int]:
    s = (s or "").strip()
    # ISO-ish: YYYY-MM-DD...
    if len(s) >= 7 and s[4] == "-" and s[7 - 1] == "-":
        try:
            m = int(s[5:7])
            return m if 1 <= m <= 12 else None
        except Exception:
            return None
    return None


def _coerce_bool(s: str, default: bool = False) -> bool:
    if s is None:
        return default
    v = str(s).strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


# ----------------------------
# IEM CSV streaming (robust)
# ----------------------------
def stream_pireps(
    session: requests.Session,
    sts: datetime,
    ets: datetime,
    timeout: Tuple[int, int],
    fmt: str = "csv",
    artcc: str = "_ALL",
    use_spatial_filter: bool = True,
    lat: float = 39.5,
    lon: float = -98.35,
    degrees: float = 30.0,
    max_retries: int = 6,
    max_raw_len: int = 500_000,  # safety: skip pathological records
) -> Iterable[Tuple[str, int]]:
    """
    Streams IEM PIREP CSV and yields (raw_text, month).

    Practical notes:
      - IEM CSV commonly includes REPORT/ICING/TURBULENCE instead of RAW.
      - Without filtering, IEM recommends requests be <=120 days per query.
      - Optional filters: artcc and/or lon/lat circle via filter=1, lat, lon, degrees.
    """
    params = {
        "fmt": fmt,
        "sts": sts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ets": ets.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "artcc": artcc,
    }

    if use_spatial_filter:
        params.update(
            {
                "filter": "1",
                "lat": f"{lat}",
                "lon": f"{lon}",
                "degrees": f"{degrees}",
            }
        )

    for attempt in range(max_retries):
        try:
            with session.get(IEM_PIREP_URL, params=params, timeout=timeout, stream=True) as r:
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

                report_idx = None
                icing_idx = None
                turb_idx = None
                ts_idx = None

                for i, n in enumerate(header_l):
                    if n == "report":
                        report_idx = i
                    elif n == "icing":
                        icing_idx = i
                    elif n == "turbulence":
                        turb_idx = i
                    elif n in ("valid", "timestamp", "time", "report_time", "obs_time", "issued", "issue_time"):
                        ts_idx = i

                if report_idx is None:
                    print(f"[PIREP] could not find REPORT column in header: {header}", file=sys.stderr)
                    return

                default_month = sts.month

                for cols in reader:
                    if not cols or report_idx >= len(cols):
                        continue

                    raw = (cols[report_idx] or "").strip()

                    # Append icing/turb fields (structured, but useful for rarity)
                    extras = []
                    if icing_idx is not None and icing_idx < len(cols):
                        v = (cols[icing_idx] or "").strip()
                        if v:
                            extras.append(f"ICING {v}")
                    if turb_idx is not None and turb_idx < len(cols):
                        v = (cols[turb_idx] or "").strip()
                        if v:
                            extras.append(f"TURB {v}")

                    if extras:
                        raw = (raw + " " + " ".join(extras)).strip()

                    if not raw:
                        continue

                    if max_raw_len and len(raw) > max_raw_len:
                        # Skip pathological megarecords rather than crashing training.
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
            print(f"[PIREP] request failed (attempt {attempt+1}/{max_retries}): {e} -> sleep {sleep_s}s", file=sys.stderr)
            time.sleep(sleep_s)

    raise RuntimeError("[PIREP] request repeatedly failed after retries")


# ----------------------------
# Training
# ----------------------------
def train(
    out_path: str,
    years: int = 5,
    alpha: float = 0.5,
    connect_timeout: int = 20,
    read_timeout: int = 180,
    window_days: int = 30,
    artcc: str = "_ALL",
    use_spatial_filter: bool = True,
    lat: float = 39.5,
    lon: float = -98.35,
    degrees: float = 30.0,
) -> None:
    """
    Train a season-aware rarity model from IEM PIREP archive.

    - Default window_days=30 (small responses; friendly to IEM)
    - If FILTER=0 and window_days>120, we clamp to 120.
    """
    now = datetime.now(UTC)
    ets = now
    sts = now - timedelta(days=365 * years)
    timeout = (connect_timeout, read_timeout)

    if not use_spatial_filter and window_days > 120:
        print("[PIREP] window_days reduced to 120 due to IEM unfiltered limit.", file=sys.stderr)
        window_days = 120

    counts_all: Dict[str, int] = defaultdict(int)
    total_all = 0
    totals_by_month: Dict[str, int] = {str(m): 0 for m in range(1, 13)}
    counts_by_month: Dict[str, Dict[str, int]] = {str(m): defaultdict(int) for m in range(1, 13)}

    with requests.Session() as session:
        print(
            f"[PIREP] Window: {sts:%Y-%m-%d} → {ets:%Y-%m-%d} (~{years}y) "
            f"| window_days={window_days} artcc={artcc} spatial_filter={use_spatial_filter} "
            f"lat={lat} lon={lon} deg={degrees}",
            file=sys.stderr,
        )

        t0 = time.time()
        cur = sts
        window = timedelta(days=window_days)
        reports = 0

        while cur < ets:
            nxt = min(ets, cur + window)
            print(f"[PIREP] {cur:%Y-%m-%d} → {nxt:%Y-%m-%d}", file=sys.stderr)

            try:
                for raw, month in stream_pireps(
                    session,
                    sts=cur,
                    ets=nxt,
                    timeout=timeout,
                    artcc=artcc,
                    use_spatial_filter=use_spatial_filter,
                    lat=lat,
                    lon=lon,
                    degrees=degrees,
                ):
                    toks = tokenize_pirep(raw)
                    norm = {normalize_token(tok) for tok in toks}
                    mkey = str(month)

                    for nt in norm:
                        counts_all[nt] += 1
                        total_all += 1
                        counts_by_month[mkey][nt] += 1
                        totals_by_month[mkey] += 1

                    reports += 1

            except Exception as e:
                print(f"[PIREP] window failed: {e}", file=sys.stderr)

            if reports and reports % 200_000 == 0:
                dt = time.time() - t0
                rate = reports / dt if dt > 0 else 0.0
                print(f"[PIREP] processed {reports:,} reports ({rate:,.0f}/s) tokens={total_all:,}", file=sys.stderr)

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
            "note": (
                "PIREP rarity model trained from IEM /cgi-bin/request/gis/pireps.py "
                f"(artcc={artcc}, spatial_filter={use_spatial_filter}, window_days={window_days})."
            ),
        }

        if out_path.endswith(".gz"):
            with gzip.open(out_path, "wt", encoding="utf-8") as f:
                json.dump(model_obj, f)
        else:
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(model_obj, f)

        print(
            f"Saved {out_path}  vocab={vocab:,} token_events={total_all:,} reports={reports:,}  ({dt/60:.1f} min)",
            file=sys.stderr,
        )


if __name__ == "__main__":
    out = os.getenv("OUT", "rarity_pirep.json.gz")
    years = int(os.getenv("YEARS", "5"))
    alpha = float(os.getenv("ALPHA", "0.5"))
    window_days = int(os.getenv("WINDOW_DAYS", "30"))

    connect_timeout = int(os.getenv("CONNECT_TIMEOUT", "20"))
    read_timeout = int(os.getenv("READ_TIMEOUT", "180"))

    artcc = os.getenv("ARTCC", "_ALL")

    use_spatial_filter = _coerce_bool(os.getenv("FILTER", "1"), default=True)
    lat = float(os.getenv("LAT", "39.5"))
    lon = float(os.getenv("LON", "-98.35"))
    degrees = float(os.getenv("DEGREES", "30.0"))

    train(
        out_path=out,
        years=years,
        alpha=alpha,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        window_days=window_days,
        artcc=artcc,
        use_spatial_filter=use_spatial_filter,
        lat=lat,
        lon=lon,
        degrees=degrees,
    )
