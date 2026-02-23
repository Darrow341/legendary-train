# backend/history_store.py
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Row:
    station: str
    score: float
    raw_text: str
    ts: float


class HistoryStore:
    """
    Stores "top rows" per product over time and exposes a 12-month rolling
    top list.

    Design goal: keep it simple, deterministic, and fast enough for small apps.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.db_path))
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product TEXT NOT NULL,
                    station TEXT NOT NULL,
                    score REAL NOT NULL,
                    raw_text TEXT NOT NULL,
                    ts REAL NOT NULL
                );
                """
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_product_ts ON history(product, ts);"
            )
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_history_product_score ON history(product, score);"
            )
            con.commit()

    def _cutoff_ts(self, months: int = 12) -> float:
        # months ~= 30.44 days each
        return time.time() - (months * 30.44 * 24 * 3600)

    def offer_rows(self, product: str, rows: Iterable[Dict[str, Any]]) -> None:
        """
        Insert rows for a given product.
        Expected row keys (best effort):
          - station (or icao)
          - score (float)
          - raw_text (or text)
        """
        now = time.time()
        payload: List[Tuple[str, str, float, str, float]] = []

        for r in rows:
            station = (r.get("station") or r.get("icao") or "").strip()
            raw_text = (r.get("raw_text") or r.get("text") or "").strip()
            score_val = r.get("score")

            if not station or score_val is None:
                continue

            try:
                score = float(score_val)
            except Exception:
                continue

            payload.append((product, station, score, raw_text, now))

        if not payload:
            return

        with self._connect() as con:
            con.executemany(
                "INSERT INTO history(product, station, score, raw_text, ts) VALUES(?,?,?,?,?)",
                payload,
            )

            # Prune old records beyond 12 months
            cutoff = self._cutoff_ts(12)
            con.execute("DELETE FROM history WHERE ts < ?", (cutoff,))
            con.commit()

    def get_top(self, product: str, top: int = 10) -> List[Dict[str, Any]]:
        """
        Returns a 12-month rolling top list for a product, aggregated by station.

        Strategy:
        - consider last 12 months of rows
        - for each station, take max(score) and the newest raw_text for that station
        - sort by max(score) desc
        """
        cutoff = self._cutoff_ts(12)

        # Clamp top to a reasonable range
        try:
            top_n = int(top)
        except Exception:
            top_n = 10
        top_n = max(1, min(top_n, 200))

        with self._connect() as con:
            # max score per station in last 12 months
            # and pick latest raw_text via correlated subquery on ts
            cur = con.execute(
                """
                SELECT
                    h.station AS station,
                    MAX(h.score) AS score,
                    (
                        SELECT h2.raw_text
                        FROM history h2
                        WHERE h2.product = h.product
                          AND h2.station = h.station
                          AND h2.ts >= ?
                        ORDER BY h2.ts DESC
                        LIMIT 1
                    ) AS raw_text,
                    MAX(h.ts) AS ts
                FROM history h
                WHERE h.product = ?
                  AND h.ts >= ?
                GROUP BY h.station
                ORDER BY score DESC
                LIMIT ?;
                """,
                (cutoff, product, cutoff, top_n),
            )
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "station": r["station"],
                    "score": float(r["score"]) if r["score"] is not None else 0.0,
                    "raw_text": r["raw_text"] or "",
                    "ts": float(r["ts"]) if r["ts"] is not None else 0.0,
                }
            )
        return out
