from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Rolling window: 12 months ~ 365 days
WINDOW_SECONDS = int(os.getenv("HISTORY_WINDOW_SECONDS", str(365 * 24 * 3600)))

# Hard cap per product (default 50)
MAX_ROWS_PER_PRODUCT = int(os.getenv("HISTORY_MAX_ROWS_PER_PRODUCT", "50"))

# DB location
DB_PATH = Path(os.getenv("HISTORY_DB_PATH", Path(__file__).resolve().parent / "data" / "history.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(str(DB_PATH))
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def init_db() -> None:
    with _connect() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              product TEXT NOT NULL,
              station TEXT NOT NULL,
              text TEXT NOT NULL,
              score REAL NOT NULL,
              lat REAL,
              lon REAL,
              obs_time_utc TEXT,
              first_seen_unix INTEGER NOT NULL,
              last_seen_unix INTEGER NOT NULL,
              seen_count INTEGER NOT NULL DEFAULT 1,
              UNIQUE(product, station, text)
            );
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_history_product_score ON history(product, score DESC);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_history_last_seen ON history(last_seen_unix);")


def _now_unix() -> int:
    return int(time.time())


def _cutoff_unix(now_unix: int) -> int:
    return now_unix - WINDOW_SECONDS


def prune(now_unix: Optional[int] = None) -> None:
    """
    Prune by:
      1) rolling window (last_seen within ~12 months)
      2) hard cap per product (keep top MAX_ROWS_PER_PRODUCT by score)
    """
    now = _now_unix() if now_unix is None else int(now_unix)
    cutoff = _cutoff_unix(now)

    with _connect() as con:
        # 1) Age prune
        con.execute("DELETE FROM history WHERE last_seen_unix < ?;", (cutoff,))

        # 2) Cap prune per product
        products = [r[0] for r in con.execute("SELECT DISTINCT product FROM history;").fetchall()]
        for p in products:
            con.execute(
                """
                DELETE FROM history
                WHERE id IN (
                  SELECT id FROM history
                  WHERE product = ?
                  ORDER BY score DESC
                  LIMIT -1 OFFSET ?
                );
                """,
                (p, MAX_ROWS_PER_PRODUCT),
            )


def offer_rows(product: str, rows: List[Dict[str, Any]], now_unix: Optional[int] = None) -> None:
    """
    Insert/update deduped (product, station, text).
    Keeps max score seen; updates last_seen/seen_count.
    """
    if not rows:
        return

    now = _now_unix() if now_unix is None else int(now_unix)

    cleaned: List[Tuple[str, str, str, float, Optional[float], Optional[float], Optional[str]]] = []
    for r in rows:
        station = str(r.get("station") or r.get("icaoId") or "----").strip() or "----"
        text = str(r.get("text") or r.get("rawOb") or r.get("raw") or "").strip()
        if not text:
            continue

        score = r.get("score")
        try:
            score_f = float(score)
        except Exception:
            continue

        lat = r.get("lat")
        lon = r.get("lon")
        lat_f = float(lat) if isinstance(lat, (int, float)) else None
        lon_f = float(lon) if isinstance(lon, (int, float)) else None

        obs_time_utc = r.get("obs_time_utc")
        obs_time_utc = str(obs_time_utc).strip() if obs_time_utc else None

        cleaned.append((product, station, text, score_f, lat_f, lon_f, obs_time_utc))

    if not cleaned:
        return

    init_db()

    with _connect() as con:
        for (p, station, text, score, lat, lon, obs_time_utc) in cleaned:
            con.execute(
                """
                INSERT INTO history (product, station, text, score, lat, lon, obs_time_utc, first_seen_unix, last_seen_unix, seen_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(product, station, text) DO UPDATE SET
                  last_seen_unix = excluded.last_seen_unix,
                  seen_count = history.seen_count + 1,
                  score = CASE WHEN excluded.score > history.score THEN excluded.score ELSE history.score END,
                  lat = COALESCE(excluded.lat, history.lat),
                  lon = COALESCE(excluded.lon, history.lon),
                  obs_time_utc = COALESCE(excluded.obs_time_utc, history.obs_time_utc);
                """,
                (p, station, text, score, lat, lon, obs_time_utc, now, now),
            )

    prune(now_unix=now)


def get_top(product: str, top: int = 25, now_unix: Optional[int] = None) -> Dict[str, Any]:
    init_db()

    now = _now_unix() if now_unix is None else int(now_unix)
    cutoff = _cutoff_unix(now)

    with _connect() as con:
        rows = con.execute(
            """
            SELECT product, station, score, text, lat, lon, obs_time_utc, first_seen_unix, last_seen_unix, seen_count
            FROM history
            WHERE product = ? AND last_seen_unix >= ?
            ORDER BY score DESC
            LIMIT ?;
            """,
            (product, cutoff, int(top)),
        ).fetchall()

    out_rows = []
    for (p, station, score, text, lat, lon, obs_time_utc, first_seen_unix, last_seen_unix, seen_count) in rows:
        out_rows.append(
            {
                "product": p,
                "station": station,
                "score": float(score),
                "text": text,
                "lat": lat,
                "lon": lon,
                "obs_time_utc": obs_time_utc,
                "first_seen_unix": int(first_seen_unix),
                "last_seen_unix": int(last_seen_unix),
                "seen_count": int(seen_count),
            }
        )

    return {
        "generated_at_unix": now,
        "product": product,
        "window_seconds": WINDOW_SECONDS,
        "max_rows_per_product": MAX_ROWS_PER_PRODUCT,
        "top": int(top),
        "count": len(out_rows),
        "rows": out_rows,
    }
