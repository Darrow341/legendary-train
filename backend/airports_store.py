from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass(frozen=True)
class Airport:
    ident: str
    name: str
    type: str
    lat: float
    lon: float
    iata: str | None


class AirportsIndex:
    """
    Lightweight spatial index for airports using a 1-degree grid.
    Good enough for fast bbox queries without extra deps.
    """

    def __init__(self, grid: Dict[Tuple[int, int], List[Airport]], total: int):
        self._grid = grid
        self.total = total

    @staticmethod
    def _cell(lat: float, lon: float) -> Tuple[int, int]:
        return (int(math.floor(lat)), int(math.floor(lon)))

    @classmethod
    def from_csv(cls, csv_path: Path) -> "AirportsIndex":
        grid: Dict[Tuple[int, int], List[Airport]] = {}
        total = 0

        with csv_path.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    lat = float(row.get("latitude_deg") or "")
                    lon = float(row.get("longitude_deg") or "")
                except Exception:
                    continue

                ident = (row.get("ident") or "").strip()
                if not ident:
                    continue

                typ = (row.get("type") or "").strip()
                name = (row.get("name") or ident).strip()
                iata = (row.get("iata_code") or "").strip() or None

                ap = Airport(
                    ident=ident,
                    name=name,
                    type=typ,
                    lat=lat,
                    lon=lon,
                    iata=iata,
                )

                c = cls._cell(lat, lon)
                grid.setdefault(c, []).append(ap)
                total += 1

        return cls(grid=grid, total=total)

    def query_bbox(
        self,
        west: float,
        south: float,
        east: float,
        north: float,
        *,
        zoom: int,
        limit: int = 1200,
    ) -> List[dict]:
        """
        Return airports within bbox. The allowed airport types depend on zoom:
          - zoom < 6: large only
          - 6..8: large + medium
          - >= 9: large + medium + small
        """
        # Normalize bbox (handle weird input)
        w = float(min(west, east))
        e = float(max(west, east))
        s = float(min(south, north))
        n = float(max(south, north))

        if zoom < 6:
            allowed = {"large_airport"}
        elif zoom < 9:
            allowed = {"large_airport", "medium_airport"}
        else:
            allowed = {"large_airport", "medium_airport", "small_airport"}

        # Clamp to world-ish bounds
        s = max(-90.0, min(90.0, s))
        n = max(-90.0, min(90.0, n))
        w = max(-180.0, min(180.0, w))
        e = max(-180.0, min(180.0, e))

        min_lat_cell = int(math.floor(s))
        max_lat_cell = int(math.floor(n))
        min_lon_cell = int(math.floor(w))
        max_lon_cell = int(math.floor(e))

        out: List[dict] = []
        seen = 0

        for lat_cell in range(min_lat_cell, max_lat_cell + 1):
            for lon_cell in range(min_lon_cell, max_lon_cell + 1):
                cell_list = self._grid.get((lat_cell, lon_cell))
                if not cell_list:
                    continue

                for ap in cell_list:
                    if ap.type not in allowed:
                        continue
                    if ap.lon < w or ap.lon > e or ap.lat < s or ap.lat > n:
                        continue

                    out.append(
                        {
                            "ident": ap.ident,
                            "name": ap.name,
                            "type": ap.type,
                            "lat": ap.lat,
                            "lon": ap.lon,
                            "iata": ap.iata,
                        }
                    )
                    seen += 1
                    if seen >= limit:
                        return out

        return out
