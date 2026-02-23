import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { Routes, Route, useNavigate, useParams, Link } from "react-router-dom";
import { MapContainer, TileLayer, Marker, Popup, Tooltip } from "react-leaflet";
import L from "leaflet";
import MarkerClusterGroup from "react-leaflet-cluster";

import "leaflet/dist/leaflet.css";
import "leaflet.markercluster/dist/MarkerCluster.css";
import "leaflet.markercluster/dist/MarkerCluster.Default.css";

// Fix missing default marker icons
import markerIcon2x from "leaflet/dist/images/marker-icon-2x.png";
import markerIcon from "leaflet/dist/images/marker-icon.png";
import markerShadow from "leaflet/dist/images/marker-shadow.png";

delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIcon2x,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

function copyText(s) {
  return navigator.clipboard.writeText(s);
}

function hashString(s) {
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = (h * 33) ^ s.charCodeAt(i);
  return (h >>> 0).toString(36);
}

function normalizeRow(r, fallbackProduct) {
  const product = (r?.product || fallbackProduct || "").toString();

  const station =
  (r?.station ?? r?.icaoId ?? r?.stationId ?? r?.station_id ?? "")
  .toString()
  .trim() || "----";

  const text = (r?.text ?? r?.rawOb ?? r?.rawTAF ?? r?.raw ?? r?.report ?? "")
  .toString()
  .trim();

  const score = typeof r?.score === "number" ? r.score : Number(r?.score);

  const lat = typeof r?.lat === "number" ? r.lat : r?.lat != null ? Number(r.lat) : null;
  const lon = typeof r?.lon === "number" ? r.lon : r?.lon != null ? Number(r.lon) : null;

  const seen_count =
  typeof r?.seen_count === "number"
  ? r.seen_count
  : r?.seen_count != null
  ? Number(r.seen_count)
  : null;

  const first_seen_unix =
  typeof r?.first_seen_unix === "number"
  ? r.first_seen_unix
  : r?.first_seen_unix != null
  ? Number(r.first_seen_unix)
  : null;

  const last_seen_unix =
  typeof r?.last_seen_unix === "number"
  ? r.last_seen_unix
  : r?.last_seen_unix != null
  ? Number(r.last_seen_unix)
  : null;

  const obs_time_utc = r?.obs_time_utc != null ? String(r.obs_time_utc) : null;

  return {
    product,
    station,
    text,
    score: Number.isFinite(score) ? score : null,
    lat: Number.isFinite(lat) ? lat : null,
    lon: Number.isFinite(lon) ? lon : null,
    seen_count: Number.isFinite(seen_count) ? seen_count : null,
    first_seen_unix: Number.isFinite(first_seen_unix) ? first_seen_unix : null,
    last_seen_unix: Number.isFinite(last_seen_unix) ? last_seen_unix : null,
    obs_time_utc,
  };
}

function rowIdentityKey(row) {
  const p = row.product || "";
  const st = row.station || "";
  const txt = row.text || "";
  const base = p === "PIREP" ? `${p}|${txt}` : `${p}|${st}|${txt}`;
  return hashString(base);
}

async function fetchJsonOrThrow(url) {
  const r = await fetch(url);
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`Fetch failed: ${url} -> HTTP ${r.status}${t ? `\n${t}` : ""}`);
  }
  return r.json();
}

async function fetchLive({ product, top, conus, hours }) {
  if (product === "METAR")
    return fetchJsonOrThrow(`/api/leaderboard?top=${top}&conus=${conus ? "true" : "false"}`);
  if (product === "TAF") return fetchJsonOrThrow(`/api/taf?top=${top}`);
  if (product === "PIREP") return fetchJsonOrThrow(`/api/pirep?top=${top}&hours=${hours}`);
  throw new Error(`Unknown product: ${product}`);
}

async function fetchHistory({ product, top }) {
  return fetchJsonOrThrow(`/api/history?product=${encodeURIComponent(product)}&top=${top}`);
}

function Layout({ children }) {
  return (
    <div
    style={{
      display: "grid",
      gridTemplateColumns: "1.25fr 1fr",
      gap: 16,
      padding: 16,
      height: "100vh",
      boxSizing: "border-box",
    }}
    >
    {children}
    </div>
  );
}

function formatAgoFromUnix(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "";
  const now = Date.now() / 1000;
  const d = Math.max(0, Math.floor(now - seconds));
  if (d < 60) return `${d}s ago`;
  const m = Math.floor(d / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ago`;
  const days = Math.floor(h / 24);
  return `${days}d ago`;
}

function TrendBadge({ kind, text, title }) {
  const stylesByKind = {
    up: { border: "1px solid #86efac", background: "#dcfce7", color: "#166534" },
    down: { border: "1px solid #fca5a5", background: "#fee2e2", color: "#7f1d1d" },
    new: { border: "1px solid #93c5fd", background: "#dbeafe", color: "#1e3a8a" },
    active: { border: "1px solid #d8b4fe", background: "#f3e8ff", color: "#581c87" },
    flat: { border: "1px solid #e5e7eb", background: "#f3f4f6", color: "#374151" },
  };

  const s = stylesByKind[kind] || stylesByKind.flat;

  return (
    <span
    title={title || ""}
    style={{
      display: "inline-block",
      fontFamily: "monospace",
      fontSize: 12,
      padding: "2px 8px",
      borderRadius: 999,
      whiteSpace: "nowrap",
      ...s,
    }}
    >
    {text}
    </span>
  );
}

function TrendLegend({ mode }) {
  return (
    <div
    style={{
      marginTop: 10,
      marginBottom: 10,
      padding: "8px 10px",
      border: "1px solid #eee",
      borderRadius: 10,
      background: "rgba(0,0,0,0.02)",
          display: "flex",
          flexWrap: "wrap",
          gap: 10,
          alignItems: "center",
          fontSize: 12,
    }}
    >
    <span style={{ opacity: 0.8 }}>Trend legend:</span>

    {mode === "LIVE" ? (
      <>
      <TrendBadge kind="up" text="↑N" title="Moved up N places since last refresh" />
      <span style={{ opacity: 0.8 }}>rank up</span>

      <TrendBadge kind="down" text="↓N" title="Moved down N places since last refresh" />
      <span style={{ opacity: 0.8 }}>rank down</span>

      <TrendBadge kind="new" text="NEW" title="New since last refresh" />
      <span style={{ opacity: 0.8 }}>new entry</span>

      <TrendBadge kind="flat" text="—" title="No change since last refresh" />
      <span style={{ opacity: 0.8 }}>no change</span>
      </>
    ) : (
      <>
      <TrendBadge kind="active" text="ACTIVE +N • Xm ago" title="Seen again since last refresh" />
      <span style={{ opacity: 0.8 }}>seen again</span>

      <TrendBadge kind="new" text="NEW" title="New in this history view" />
      <span style={{ opacity: 0.8 }}>new to list</span>

      <TrendBadge kind="flat" text="—" title="No new activity since last refresh" />
      <span style={{ opacity: 0.8 }}>no new activity</span>
      </>
    )}
    </div>
  );
}

function makeRankIcon(rank) {
  const r = Number(rank);
  const label = Number.isFinite(r) && r > 0 ? String(r) : "?";

  const fontSize = label.length >= 3 ? 11 : label.length === 2 ? 12 : 13;
  const size = 30;

  const bg = r === 1 ? "#fde68a" : r === 2 ? "#e5e7eb" : r === 3 ? "#fecaca" : "#ffffff";
  const border = r === 1 ? "#f59e0b" : r === 2 ? "#9ca3af" : r === 3 ? "#ef4444" : "#111827";

  const html = `
  <div style="
  width:${size}px;
  height:${size}px;
  border-radius:999px;
  background:${bg};
  border:2px solid ${border};
  display:flex;
  align-items:center;
  justify-content:center;
  box-shadow:0 2px 6px rgba(0,0,0,0.25);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
  font-weight: 800;
  font-size:${fontSize}px;
  color:#111827;
  line-height:1;
  user-select:none;
  ">${label}</div>
  `;

  return L.divIcon({
    className: "rank-marker-icon",
    html,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
    popupAnchor: [0, -size / 2],
  });
}

function makeBestRankClusterIcon(bestRank, count) {
  const r = Number(bestRank);
  const c = Number(count);

  const bestLabel = Number.isFinite(r) && r > 0 ? `#${r}` : "#?";
  const countLabel = Number.isFinite(c) ? String(c) : "?";

  const size = c >= 100 ? 56 : c >= 25 ? 52 : c >= 10 ? 48 : 44;
  const fontSize = 12;

  const bg = r === 1 ? "#fde68a" : r === 2 ? "#e5e7eb" : r === 3 ? "#fecaca" : "#ffffff";
  const border = r === 1 ? "#f59e0b" : r === 2 ? "#9ca3af" : r === 3 ? "#ef4444" : "#111827";

  const html = `
  <div style="
  width:${size}px;
  height:${size}px;
  border-radius:999px;
  background:${bg};
  border:2px solid ${border};
  display:flex;
  flex-direction:column;
  align-items:center;
  justify-content:center;
  box-shadow:0 2px 10px rgba(0,0,0,0.25);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
  user-select:none;
  line-height:1.05;
  ">
  <div style="font-weight:900; font-size:${fontSize + 1}px; color:#111827;">${bestLabel}</div>
  <div style="font-weight:700; font-size:${fontSize}px; opacity:0.85; color:#111827;">${countLabel}</div>
  </div>
  `;

  return L.divIcon({
    className: "best-rank-cluster-icon",
    html,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

// Dark basemap: CARTO "Dark Matter"
const DARK_TILE_URL = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png";
const DARK_TILE_ATTRIB =
'&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>';

function HomePage({
  top,
  setTop,
  product,
  setProduct,
  pirepHours,
  setPirepHours,
  conusOnly,
  setConusOnly,
  radarOn,
  setRadarOn,
}) {
  const [mode, setMode] = useState("LIVE"); // LIVE | HISTORY
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [lastRefreshAt, setLastRefreshAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [clusterOn, setClusterOn] = useState(true);

  // Radar nonce for cache-busting; updates every 30s
  const [radarNonce, setRadarNonce] = useState(() => Math.floor(Date.now() / 30_000));

  const navigate = useNavigate();

  const prevLiveRanksRef = useRef(new Map());
  const prevHistActivityRef = useRef(new Map());
  const trendByKeyRef = useRef(new Map());

  const mapCenter = useMemo(() => [39.5, -98.35], []);

  const rankByKey = useMemo(() => {
    const m = new Map();
    rows.forEach((r, idx) => {
      m.set(rowIdentityKey(r), idx + 1);
    });
    return m;
  }, [rows]);

  const markers = useMemo(
    () => rows.filter((r) => typeof r.lat === "number" && typeof r.lon === "number"),
                          [rows]
  );

  useEffect(() => {
    if (!radarOn) return;
    const t = setInterval(() => {
      setRadarNonce(Math.floor(Date.now() / 30_000));
    }, 30_000);
    return () => clearInterval(t);
  }, [radarOn]);

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");

    try {
      const data =
      mode === "HISTORY"
      ? await fetchHistory({ product, top })
      : await fetchLive({ product, top, conus: conusOnly, hours: pirepHours });

      const normalized = Array.isArray(data?.rows)
      ? data.rows.map((r) => normalizeRow(r, product)).filter((r) => r.text)
      : [];

      const trends = new Map();

      if (mode === "LIVE") {
        const scopeKey = `LIVE|${product}|${conusOnly ? "conus" : "all"}|${
          product === "PIREP" ? pirepHours : "na"
        }|${top}`;
        const prevRanks = prevLiveRanksRef.current.get(scopeKey) || new Map();
        const nextRanks = new Map();

        normalized.forEach((r, idx) => {
          const key = rowIdentityKey(r);
          const rank = idx + 1;
          nextRanks.set(key, rank);

          if (!prevRanks.has(key)) {
            trends.set(key, { kind: "new", text: "NEW", title: "New since last refresh" });
          } else {
            const prev = prevRanks.get(key);
            const delta = prev - rank;
            if (delta > 0) trends.set(key, { kind: "up", text: `↑${delta}`, title: `Up ${delta} since last refresh` });
            else if (delta < 0)
              trends.set(key, {
                kind: "down",
                text: `↓${Math.abs(delta)}`,
                         title: `Down ${Math.abs(delta)} since last refresh`,
              });
            else trends.set(key, { kind: "flat", text: "—", title: "No rank change since last refresh" });
          }
        });

        prevLiveRanksRef.current.set(scopeKey, nextRanks);
      } else {
        const scopeKey = `HISTORY|${product}|${top}`;
        const prev = prevHistActivityRef.current.get(scopeKey) || new Map();
        const next = new Map();

        normalized.forEach((r) => {
          const key = rowIdentityKey(r);

          const seen = Number.isFinite(r.seen_count) ? r.seen_count : null;
          const lastSeen = Number.isFinite(r.last_seen_unix) ? r.last_seen_unix : null;

          next.set(key, { seen_count: seen, last_seen_unix: lastSeen });

          const prevEntry = prev.get(key);
          const ago = lastSeen != null ? formatAgoFromUnix(lastSeen) : "";

          if (!prevEntry) {
            trends.set(key, { kind: "new", text: "NEW", title: `New in this view${ago ? ` • last seen ${ago}` : ""}` });
            return;
          }

          const prevSeen = prevEntry.seen_count;
          const prevLast = prevEntry.last_seen_unix;

          const seenDelta = Number.isFinite(seen) && Number.isFinite(prevSeen) ? seen - prevSeen : null;
          const lastAdvanced = Number.isFinite(lastSeen) && Number.isFinite(prevLast) ? lastSeen > prevLast : false;

          if ((seenDelta != null && seenDelta > 0) || lastAdvanced) {
            const parts = [];
            if (seenDelta != null && seenDelta > 0) parts.push(`+${seenDelta}`);
            if (ago) parts.push(ago);
            const label = parts.length ? `ACTIVE ${parts.join(" • ")}` : "ACTIVE";
            trends.set(key, { kind: "active", text: label, title: "Seen again since last refresh" });
          } else {
            trends.set(key, {
              kind: "flat",
              text: "—",
              title: ago ? `No new activity • last seen ${ago}` : "No new activity since last refresh",
            });
          }
        });

        prevHistActivityRef.current.set(scopeKey, next);
      }

      trendByKeyRef.current = trends;
      setRows(normalized);

      if (data?.generated_at_utc) setGeneratedAt(`UTC: ${data.generated_at_utc}`);
      else if (typeof data?.generated_at_unix === "number")
        setGeneratedAt(`generated: ${new Date(data.generated_at_unix * 1000).toISOString()}`);
      else setGeneratedAt(null);

      setLastRefreshAt(new Date());
    } catch (e) {
      setErr(String(e));
      setRows([]);
      setGeneratedAt(null);
      setLastRefreshAt(new Date());
      trendByKeyRef.current = new Map();
    } finally {
      setLoading(false);
    }
  }, [mode, product, top, conusOnly, pirepHours]);

  useEffect(() => {
    trendByKeyRef.current = new Map();
    load();
  }, [load]);

  function onRowClick(row) {
    const p = row.product || product;
    if (p === "PIREP") return;
    const st = (row.station || "").trim();
    if (!st || st === "----") return;
    navigate(`/station/${st}?product=${encodeURIComponent(p)}`);
  }

  const clusterIconCreateFunction = useCallback((cluster) => {
    const children = cluster.getAllChildMarkers();
    let best = Infinity;
    for (const m of children) {
      const r = m?.options?.__rank;
      if (Number.isFinite(r) && r > 0) best = Math.min(best, r);
    }
    const count = cluster.getChildCount();
    const bestRank = Number.isFinite(best) && best !== Infinity ? best : null;

    const icon = makeBestRankClusterIcon(bestRank, count);
    icon.options.__bestRank = bestRank;
    icon.options.__count = count;
    return icon;
  }, []);

  const attachClusterTooltipTitle = useCallback((cluster) => {
    const icon = cluster.getIcon?.();
    const bestRank = icon?.options?.__bestRank;
    const count = icon?.options?.__count ?? cluster.getChildCount?.();

    const bestLabel = Number.isFinite(bestRank) && bestRank > 0 ? `Best #${bestRank}` : "Best #?";
    const countLabel = Number.isFinite(count) ? `${count} stations` : "? stations";
    const title = `${bestLabel} • ${countLabel}`;

    const el = cluster.getElement?.();
    if (el) el.setAttribute("title", title);
  }, []);

    const onClusterClick = useCallback((e) => {
      const map = e?.target?._map;
      const layer = e?.layer;
      if (!map || !layer) return;

      const currentZoom = map.getZoom();
      const maxZoom = map.getMaxZoom?.() ?? 18;

      if (currentZoom >= maxZoom - 1) {
        if (typeof layer.spiderfy === "function") layer.spiderfy();
        return;
      }

      const center = layer.getLatLng?.();
      if (!center) return;

      map.setView(center, currentZoom + 1, { animate: true });
    }, []);

    return (
      <Layout>
      {/* Leaderboard */}
      <div style={{ overflow: "auto", border: "1px solid #ddd", borderRadius: 12, padding: 12 }}>
      <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "space-between",
        gap: 12,
        marginBottom: 8,
        flexWrap: "wrap",
      }}
      >
      <div>
      <div style={{ fontSize: 18, fontWeight: 700 }}>Aviation Weather Leaderboard</div>
      <div style={{ fontSize: 12, opacity: 0.75 }}>
      {mode === "HISTORY" ? "Mode: 12mo Best" : "Mode: Live"}
      {generatedAt ? ` • ${generatedAt}` : ""}
      {loading ? " • loading…" : ""}
      </div>
      </div>

      <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 8 }}>
      <div style={{ fontSize: 12, opacity: 0.75 }}>
      {lastRefreshAt ? `Last refresh: ${lastRefreshAt.toLocaleString()}` : ""}
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
      <label style={{ fontSize: 12, opacity: 0.8 }}>Mode</label>
      <select value={mode} onChange={(e) => setMode(e.target.value)}>
      <option value="LIVE">Live</option>
      <option value="HISTORY">12mo Best</option>
      </select>

      <label style={{ fontSize: 12, opacity: 0.8 }}>Product</label>
      <select value={product} onChange={(e) => setProduct(e.target.value)}>
      <option value="METAR">METAR</option>
      <option value="TAF">TAF</option>
      <option value="PIREP">PIREP</option>
      </select>

      {mode === "LIVE" && product === "METAR" ? (
        <>
        <label style={{ fontSize: 12, opacity: 0.8 }}>CONUS</label>
        <input type="checkbox" checked={conusOnly} onChange={(e) => setConusOnly(e.target.checked)} />
        </>
      ) : null}

      {mode === "LIVE" && product === "PIREP" ? (
        <>
        <label style={{ fontSize: 12, opacity: 0.8 }}>Hours</label>
        <select value={pirepHours} onChange={(e) => setPirepHours(Number(e.target.value))}>
        {[3, 6, 12, 24, 48, 72].map((h) => (
          <option key={h} value={h}>
          {h}
          </option>
        ))}
        </select>
        </>
      ) : null}

      <label style={{ fontSize: 12, opacity: 0.8 }}>Top</label>
      <select value={top} onChange={(e) => setTop(Number(e.target.value))}>
      {[10, 25, 50].map((n) => (
        <option key={n} value={n}>
        {n}
        </option>
      ))}
      </select>

      <label style={{ fontSize: 12, opacity: 0.8 }}>Radar</label>
      <input type="checkbox" checked={radarOn} onChange={(e) => setRadarOn(e.target.checked)} />

      <label style={{ fontSize: 12, opacity: 0.8 }}>Cluster</label>
      <input type="checkbox" checked={clusterOn} onChange={(e) => setClusterOn(e.target.checked)} />

      <button onClick={load} style={{ padding: "6px 10px" }}>
      Refresh
      </button>
      </div>
      </div>
      </div>

      <TrendLegend mode={mode} />

      {err ? <div style={{ color: "crimson", fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{err}</div> : null}

      <table style={{ width: "100%", borderCollapse: "collapse" }}>
      <thead>
      {mode === "HISTORY" ? (
        <tr style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>
        <th style={{ padding: 8 }}>#</th>
        <th style={{ padding: 8 }}>Trend</th>
        <th style={{ padding: 8 }}>Seen</th>
        <th style={{ padding: 8 }}>Last seen</th>
        <th style={{ padding: 8 }}>Station</th>
        <th style={{ padding: 8 }}>Text</th>
        </tr>
      ) : (
        <tr style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>
        <th style={{ padding: 8 }}>#</th>
        <th style={{ padding: 8 }}>Trend</th>
        <th style={{ padding: 8 }}>Station</th>
        <th style={{ padding: 8 }}>Text</th>
        </tr>
      )}
      </thead>

      <tbody>
      {rows.map((r, i) => {
        const p = r.product || product;
        const keyId = rowIdentityKey(r);
        const trend = trendByKeyRef.current.get(keyId) || { kind: "flat", text: "—", title: "" };
        const clickable = p !== "PIREP" && r.station !== "----";
        const lastSeenAgo = r.last_seen_unix != null ? formatAgoFromUnix(r.last_seen_unix) : "";

        return (
          <tr
          key={`${mode}-${p}-${r.station}-${i}`}
          onClick={() => (clickable ? onRowClick(r) : undefined)}
          style={{
            borderBottom: "1px solid #eee",
            verticalAlign: "top",
            cursor: clickable ? "pointer" : "default",
          }}
          >
          {mode === "HISTORY" ? (
            <>
            <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
            <td style={{ padding: 8 }}>
            <TrendBadge kind={trend.kind} text={trend.text} title={trend.title} />
            </td>
            <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>
            {r.seen_count != null ? r.seen_count : ""}
            </td>
            <td style={{ padding: 8, fontFamily: "monospace" }}>{lastSeenAgo}</td>
            <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
            <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{r.text}</td>
            </>
          ) : (
            <>
            <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
            <td style={{ padding: 8 }}>
            <TrendBadge kind={trend.kind} text={trend.text} title={trend.title} />
            </td>
            <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
            <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{r.text}</td>
            </>
          )}
          </tr>
        );
      })}
      </tbody>
      </table>
      </div>

      {/* Map */}
      <div style={{ border: "1px solid #ddd", borderRadius: 12, overflow: "hidden" }}>
      <MapContainer center={mapCenter} zoom={4} style={{ height: "100%", width: "100%" }}>
      {/* Dark basemap */}
      <TileLayer attribution={DARK_TILE_ATTRIB} url={DARK_TILE_URL} />

      {/* NEXRAD/MRMS reflectivity proxy tiles from our backend */}
      {radarOn ? (
        <TileLayer
        key={`radar-${radarNonce}`}
        url={`/api/radar/tiles/{z}/{x}/{y}.png?t=${radarNonce}`}
        opacity={0.55}
        zIndex={10}
        />
      ) : null}

      {clusterOn ? (
        <MarkerClusterGroup
        chunkedLoading
        showCoverageOnHover={false}
        spiderfyOnMaxZoom
        removeOutsideVisibleBounds
        iconCreateFunction={clusterIconCreateFunction}
        maxClusterRadius={50}
        eventHandlers={{
          clusterclick: onClusterClick,
          clusteradd: (e) => attachClusterTooltipTitle(e.layer),
                    clustermouseover: (e) => attachClusterTooltipTitle(e.layer),
                    clustermouseout: (e) => attachClusterTooltipTitle(e.layer),
        }}
        >
        {markers.map((r, i) => {
          const keyId = rowIdentityKey(r);
          const trend = trendByKeyRef.current.get(keyId) || { kind: "flat", text: "—", title: "" };

          const rank = rankByKey.get(keyId) || null;
          const rankLabel = rank != null ? `#${rank}` : "";
          const zIndexOffset = rank != null ? 10000 - rank : 0;

          return (
            <Marker
            key={`${r.product}-${r.station}-${i}`}
            position={[r.lat, r.lon]}
            icon={rank != null ? makeRankIcon(rank) : undefined}
            zIndexOffset={zIndexOffset}
            ref={(m) => {
              if (m && rank != null) m.options.__rank = rank;
            }}
            >
            <Tooltip direction="top" offset={[0, -12]} opacity={0.95}>
            <span style={{ fontFamily: "monospace" }}>
            {rankLabel} {r.station}
            </span>
            </Tooltip>

            <Popup>
            <div style={{ fontFamily: "monospace" }}>
            <div style={{ fontWeight: 700, marginBottom: 6 }}>
            {rankLabel ? `${rankLabel} ` : ""}
            {r.station}{" "}
            <span style={{ marginLeft: 6 }}>
            <TrendBadge kind={trend.kind} text={trend.text} title={trend.title} />
            </span>
            </div>

            <div style={{ whiteSpace: "pre-wrap" }}>{r.text}</div>

            <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button onClick={() => copyText(r.text)} style={{ padding: "6px 10px", cursor: "pointer" }}>
            Copy text
            </button>

            {r.product !== "PIREP" && r.station !== "----" ? (
              <button
              onClick={() => {
                const link = `${window.location.origin}/station/${encodeURIComponent(
                  r.station
                )}?product=${encodeURIComponent(r.product)}`;
                copyText(link);
              }}
              style={{ padding: "6px 10px", cursor: "pointer" }}
              >
              Copy link
              </button>
            ) : null}
            </div>
            </div>
            </Popup>
            </Marker>
          );
        })}
        </MarkerClusterGroup>
      ) : null}
      </MapContainer>
      </div>
      </Layout>
    );
}

function StationPage() {
  const { station } = useParams();
  const navigate = useNavigate();

  const [product, setProduct] = useState("METAR");
  const [top, setTop] = useState(25);

  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    const qs = new URLSearchParams(window.location.search);
    const p = qs.get("product");
    if (p === "METAR" || p === "TAF" || p === "PIREP") setProduct(p);
  }, []);

    const load = useCallback(async () => {
      setLoading(true);
      setErr("");
      try {
        const p = product === "PIREP" ? "METAR" : product;

        const data = await fetchLive({
          product: p,
          top,
          conus: false,
          hours: 24,
        });

        const normalized = Array.isArray(data?.rows)
        ? data.rows.map((r) => normalizeRow(r, p)).filter((r) => r.station === station)
        : [];

        setRows(normalized);
        setGeneratedAt(data?.generated_at_utc ? `UTC: ${data.generated_at_utc}` : null);
      } catch (e) {
        setErr(String(e));
        setRows([]);
        setGeneratedAt(null);
      } finally {
        setLoading(false);
      }
    }, [product, station, top]);

    useEffect(() => {
      load();
    }, [load]);

    return (
      <div style={{ padding: 16, maxWidth: 1100, margin: "0 auto" }}>
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
      <div>
      <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>{station}</div>
      <div style={{ fontSize: 12, opacity: 0.75 }}>
      {generatedAt ? generatedAt : ""}
      {loading ? " • loading…" : ""}
      </div>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
      <label style={{ fontSize: 12, opacity: 0.8 }}>Product</label>
      <select value={product} onChange={(e) => setProduct(e.target.value)}>
      <option value="METAR">METAR</option>
      <option value="TAF">TAF</option>
      </select>

      <label style={{ fontSize: 12, opacity: 0.8 }}>Top</label>
      <select value={top} onChange={(e) => setTop(Number(e.target.value))}>
      {[10, 25, 50].map((n) => (
        <option key={n} value={n}>
        {n}
        </option>
      ))}
      </select>

      <button onClick={() => navigate("/")} style={{ padding: "6px 10px" }}>
      Home
      </button>
      <button onClick={load} style={{ padding: "6px 10px" }}>
      Refresh
      </button>
      </div>
      </div>

      {err ? (
        <div style={{ marginTop: 12, color: "crimson", fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{err}</div>
      ) : null}

      <div style={{ marginTop: 12 }}>
      {rows.length === 0 ? (
        <div style={{ opacity: 0.75 }}>No rows for this station in the current fetch.</div>
      ) : (
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
        <tr style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>
        <th style={{ padding: 8 }}>#</th>
        <th style={{ padding: 8 }}>Text</th>
        </tr>
        </thead>
        <tbody>
        {rows.map((r, i) => (
          <tr key={`${r.product}-${r.station}-${i}`} style={{ borderBottom: "1px solid #eee", verticalAlign: "top" }}>
          <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
          <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{r.text}</td>
          </tr>
        ))}
        </tbody>
        </table>
      )}
      </div>

      <div style={{ marginTop: 16 }}>
      <Link to="/">← Back to leaderboard</Link>
      </div>
      </div>
    );
}

export default function App() {
  const [top, setTop] = useState(25);
  const [product, setProduct] = useState("METAR");
  const [pirepHours, setPirepHours] = useState(24);
  const [conusOnly, setConusOnly] = useState(true);
  const [radarOn, setRadarOn] = useState(true);

  return (
    <Routes>
    <Route
    path="/"
    element={
      <HomePage
      top={top}
      setTop={setTop}
      product={product}
      setProduct={setProduct}
      pirepHours={pirepHours}
      setPirepHours={setPirepHours}
      conusOnly={conusOnly}
      setConusOnly={setConusOnly}
      radarOn={radarOn}
      setRadarOn={setRadarOn}
      />
    }
    />
    <Route path="/station/:station" element={<StationPage />} />
    </Routes>
  );
}
