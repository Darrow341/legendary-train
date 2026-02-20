// App.jsx (DROP-IN REPLACEMENT)

import { useEffect, useMemo, useRef, useState } from "react";
import { Routes, Route, useNavigate, useParams, Link } from "react-router-dom";
import { MapContainer, TileLayer, Marker, Popup, LayersControl } from "react-leaflet";
import L from "leaflet";

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

function formatScore(x) {
  return typeof x === "number" ? x.toFixed(2) : "";
}

function getRowText(r) {
  // Support both old backend ("metar") and new backend ("text")
  return (r?.text || r?.metar || "").toString();
}

async function fetchLeaderboard(top) {
  const r = await fetch(`/api/leaderboard?top=${top}&conus=true`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

async function fetchRainViewerLatestTileUrl({ size = 256, color = 2, options = "1_1" } = {}) {
  const r = await fetch("https://api.rainviewer.com/public/weather-maps.json");
  if (!r.ok) throw new Error(`RainViewer HTTP ${r.status}`);
  const data = await r.json();

  const host = data?.host;
  const frames = data?.radar?.past;
  const latest = Array.isArray(frames) && frames.length ? frames[frames.length - 1] : null;
  const path = latest?.path;

  if (!host || !path) throw new Error("RainViewer response missing host/path");

  // Template: {host}{path}/{size}/{z}/{x}/{y}/{color}/{options}.png
  return `${host}${path}/${size}/{z}/{x}/{y}/${color}/${options}.png`;
}

function copyText(s) {
  return navigator.clipboard.writeText(s);
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

function HomePage({ top, setTop }) {
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  // Radar overlay state
  const [radarUrl, setRadarUrl] = useState(null);
  const [radarErr, setRadarErr] = useState("");

  const navigate = useNavigate();
  const mapRef = useRef(null);

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const markers = rows.filter((r) => typeof r.lat === "number" && typeof r.lon === "number");

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const data = await fetchLeaderboard(top);
      setRows(data.rows || []);
      setGeneratedAt(data.generated_at_utc || null);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  // Load leaderboard periodically
  useEffect(() => {
    load();
    const t = setInterval(load, 60_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [top]);

  // Load radar periodically
  useEffect(() => {
    let alive = true;

    async function loadRadar() {
      try {
        setRadarErr("");
        const url = await fetchRainViewerLatestTileUrl({ size: 256, color: 2, options: "1_1" });
        if (alive) setRadarUrl(url);
      } catch (e) {
        if (alive) setRadarErr(String(e));
      }
    }

    loadRadar();
    const t = setInterval(loadRadar, 120_000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  function onRowClick(station) {
    navigate(`/station/${station}`);
  }

  return (
    <Layout>
    {/* Leaderboard */}
    <div style={{ overflow: "auto", border: "1px solid #ddd", borderRadius: 12, padding: 12 }}>
    <div
    style={{
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: 12,
      marginBottom: 12,
    }}
    >
    <div>
    <div style={{ fontSize: 18, fontWeight: 700 }}>METAR Leaderboard (CONUS)</div>
    <div style={{ fontSize: 12, opacity: 0.75 }}>
    {generatedAt ? `UTC: ${generatedAt}` : ""}
    {loading ? " • loading…" : ""}
    </div>
    {radarErr ? (
      <div style={{ fontSize: 12, opacity: 0.75, color: "crimson" }}>
      Radar: {radarErr}
      </div>
    ) : null}
    </div>

    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
    <label style={{ fontSize: 12, opacity: 0.8 }}>Top</label>
    <select value={top} onChange={(e) => setTop(Number(e.target.value))}>
    {[10, 25, 50, 100].map((n) => (
      <option key={n} value={n}>
      {n}
      </option>
    ))}
    </select>
    <button onClick={load} style={{ padding: "6px 10px" }}>
    Refresh
    </button>
    </div>
    </div>

    {err ? (
      <div style={{ color: "crimson", fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{err}</div>
    ) : null}

    <table style={{ width: "100%", borderCollapse: "collapse" }}>
    <thead>
    <tr style={{ textAlign: "left", borderBottom: "1px solid #ddd" }}>
    <th style={{ padding: 8 }}>#</th>
    <th style={{ padding: 8 }}>Station</th>
    <th style={{ padding: 8 }}>Score</th>
    <th style={{ padding: 8 }}>METAR</th>
    </tr>
    </thead>
    <tbody>
    {rows.map((r, i) => (
      <tr
      key={`${r.station}-${i}`}
      onClick={() => onRowClick(r.station)}
      style={{
        borderBottom: "1px solid #eee",
        verticalAlign: "top",
        cursor: "pointer",
      }}
      title="Click for a shareable link"
      >
      <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
      <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
      <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{formatScore(r.score)}</td>
      <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>
      {getRowText(r)}
      </td>
      </tr>
    ))}
    </tbody>
    </table>
    </div>

    {/* Map */}
    <div style={{ border: "1px solid #ddd", borderRadius: 12, overflow: "hidden" }}>
    <MapContainer
    center={mapCenter}
    zoom={4}
    style={{ height: "100%", width: "100%" }}
    whenCreated={(map) => {
      mapRef.current = map;
    }}
    >
    <LayersControl position="topright">
    <LayersControl.BaseLayer checked name="OpenStreetMap">
    <TileLayer
    attribution="&copy; OpenStreetMap contributors"
    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    />
    </LayersControl.BaseLayer>

    <LayersControl.Overlay checked={false} name="Weather radar (RainViewer)">
    {radarUrl ? (
      <TileLayer
      url={radarUrl}
      opacity={0.55}
      zIndex={500}
      // RainViewer radar tiles are limited in zoom (commonly 7)
      maxZoom={7}
      />
    ) : (
      <></>
    )}
    </LayersControl.Overlay>
    </LayersControl>

    {markers.map((r, i) => (
      <Marker key={`${r.station}-${i}`} position={[r.lat, r.lon]}>
      <Popup>
      <div style={{ fontFamily: "monospace" }}>
      <div>
      <b>{r.station}</b> — score {formatScore(r.score)}
      </div>
      <div style={{ whiteSpace: "pre-wrap" }}>{getRowText(r)}</div>
      <div style={{ marginTop: 8 }}>
      <Link to={`/station/${r.station}`}>Open link</Link>
      </div>
      </div>
      </Popup>
      </Marker>
    ))}
    </MapContainer>
    </div>
    </Layout>
  );
}

function StationPage() {
  const { icao } = useParams();
  const navigate = useNavigate();

  const [top] = useState(100); // fetch bigger list to increase odds of finding the station
  const [rows, setRows] = useState([]);
  const [err, setErr] = useState("");

  // Radar overlay state
  const [radarUrl, setRadarUrl] = useState(null);
  const [radarErr, setRadarErr] = useState("");

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const mapRef = useRef(null);

  useEffect(() => {
    (async () => {
      try {
        const data = await fetchLeaderboard(top);
        setRows(data.rows || []);
      } catch (e) {
        setErr(String(e));
      }
    })();
  }, [top]);

  // Load radar periodically (Station page too)
  useEffect(() => {
    let alive = true;

    async function loadRadar() {
      try {
        setRadarErr("");
        const url = await fetchRainViewerLatestTileUrl({ size: 256, color: 2, options: "1_1" });
        if (alive) setRadarUrl(url);
      } catch (e) {
        if (alive) setRadarErr(String(e));
      }
    }

    loadRadar();
    const t = setInterval(loadRadar, 120_000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  const row = rows.find((r) => (r.station || "").toUpperCase() === (icao || "").toUpperCase());

  useEffect(() => {
    if (!row) return;
    if (typeof row.lat !== "number" || typeof row.lon !== "number") return;
    if (!mapRef.current) return;
    mapRef.current.setView([row.lat, row.lon], 7);
  }, [row]);

  async function onCopy() {
    await copyText(window.location.href);
    alert("Link copied!");
  }

  return (
    <Layout>
    <div style={{ overflow: "auto", border: "1px solid #ddd", borderRadius: 12, padding: 12 }}>
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12 }}>
    <div>
    <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>Station: {icao}</div>
    <div style={{ fontSize: 12, opacity: 0.75 }}>Shareable permalink page</div>
    {radarErr ? (
      <div style={{ fontSize: 12, opacity: 0.75, color: "crimson" }}>
      Radar: {radarErr}
      </div>
    ) : null}
    </div>
    <div style={{ display: "flex", gap: 8 }}>
    <button onClick={() => navigate("/")}>Back</button>
    <button onClick={onCopy}>Copy link</button>
    </div>
    </div>

    {err ? <div style={{ color: "crimson", fontFamily: "monospace" }}>{err}</div> : null}

    {!row ? (
      <div style={{ fontFamily: "monospace" }}>Station not found in the current top {top}. Try again later.</div>
    ) : (
      <div style={{ fontFamily: "monospace", whiteSpace: "pre-wrap" }}>
      <div>
      <b>Score:</b> {formatScore(row.score)}
      </div>
      <div style={{ marginTop: 8 }}>
      <b>METAR:</b>
      </div>
      <div>{getRowText(row)}</div>
      {typeof row.lat === "number" && typeof row.lon === "number" ? (
        <div style={{ marginTop: 8 }}>
        <b>Lat/Lon:</b> {row.lat}, {row.lon}
        </div>
      ) : null}
      </div>
    )}
    </div>

    <div style={{ border: "1px solid #ddd", borderRadius: 12, overflow: "hidden" }}>
    <MapContainer
    center={mapCenter}
    zoom={4}
    style={{ height: "100%", width: "100%" }}
    whenCreated={(map) => {
      mapRef.current = map;
    }}
    >
    <LayersControl position="topright">
    <LayersControl.BaseLayer checked name="OpenStreetMap">
    <TileLayer
    attribution="&copy; OpenStreetMap contributors"
    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    />
    </LayersControl.BaseLayer>

    <LayersControl.Overlay checked={false} name="Weather radar (RainViewer)">
    {radarUrl ? (
      <TileLayer url={radarUrl} opacity={0.55} zIndex={500} maxZoom={7} />
    ) : (
      <></>
    )}
    </LayersControl.Overlay>
    </LayersControl>

    {row && typeof row.lat === "number" && typeof row.lon === "number" ? (
      <Marker position={[row.lat, row.lon]}>
      <Popup>
      <div style={{ fontFamily: "monospace" }}>
      <b>{row.station}</b> — {formatScore(row.score)}
      <div style={{ whiteSpace: "pre-wrap" }}>{getRowText(row)}</div>
      </div>
      </Popup>
      </Marker>
    ) : null}
    </MapContainer>
    </div>
    </Layout>
  );
}

export default function App() {
  const [top, setTop] = useState(25);

  return (
    <Routes>
    <Route path="/" element={<HomePage top={top} setTop={setTop} />} />
    <Route path="/station/:icao" element={<StationPage />} />
    </Routes>
  );
}
