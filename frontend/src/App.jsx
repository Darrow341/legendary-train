import { useEffect, useMemo, useRef, useState } from "react";
import { Routes, Route, useNavigate, useParams, Link, Navigate } from "react-router-dom";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
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

function copyText(s) {
  return navigator.clipboard.writeText(s);
}

/**
 * Backend endpoints:
 * - METAR: /api/leaderboard?top=25&conus=true
 * - TAF:   /api/taf?top=25
 * - PIREP: /api/pirep?top=25&hours=24   (we’ll filter ARP client-side unless backend already does)
 */
async function fetchProduct({ product, top, hours }) {
  let url = "";
  if (product === "METAR") {
    url = `/api/leaderboard?top=${top}&conus=true`;
  } else if (product === "TAF") {
    url = `/api/taf?top=${top}`;
  } else if (product === "PIREP") {
    // If your backend supports pilot-only filtering via a query param, add it here.
    // Example (if implemented): `&pilot=true`
    url = `/api/pirep?top=${top}&hours=${hours}`;
  } else {
    throw new Error(`Unknown product: ${product}`);
  }

  const r = await fetch(url);
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status}${txt ? ` — ${txt}` : ""}`);
  }
  return r.json();
}

// client-side filter to remove ARP position reports from PIREP list
function filterPilotReportsOnly(rows) {
  return (rows || []).filter((r) => {
    const t = (r?.text || "").trim().toUpperCase();
    if (!t) return false;
    // ARP = aircraft position report, not a classic UA/UUA PIREP
    if (t.startsWith("ARP ")) return false;
    return true;
  });
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
  const [product, setProduct] = useState("METAR"); // METAR | TAF | PIREP
  const [hours, setHours] = useState(24); // PIREP only

  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const navigate = useNavigate();
  const mapRef = useRef(null);

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const markers = (rows || []).filter((r) => typeof r.lat === "number" && typeof r.lon === "number");

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const data = await fetchProduct({ product, top, hours });

      let nextRows = data.rows || [];
      // PIREP: filter ARP out unless backend already does
      if (product === "PIREP") nextRows = filterPilotReportsOnly(nextRows);

      setRows(nextRows);
      setGeneratedAt(data.generated_at_utc || null);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const t = setInterval(load, 60_000);
    return () => clearInterval(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [top, product, hours]);

  function onRowClick(row, index) {
    // Station permalinks only make sense for METAR/TAF (station is meaningful)
    if (product === "PIREP") return;
    navigate(`/station/${encodeURIComponent(row.station)}`);
  }

  const title =
  product === "METAR"
  ? "METAR Leaderboard (CONUS)"
  : product === "TAF"
  ? "TAF Leaderboard (Global)"
  : `PIREP Leaderboard (Last ${hours}h — Pilot reports only)`;

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
      flexWrap: "wrap",
    }}
    >
    <div>
    <div style={{ fontSize: 18, fontWeight: 700 }}>{title}</div>
    <div style={{ fontSize: 12, opacity: 0.75 }}>
    {generatedAt ? `UTC: ${generatedAt}` : ""}
    {loading ? " • loading…" : ""}
    </div>
    </div>

    <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
    <label style={{ fontSize: 12, opacity: 0.8 }}>Product</label>
    <select value={product} onChange={(e) => setProduct(e.target.value)}>
    {["METAR", "TAF", "PIREP"].map((p) => (
      <option key={p} value={p}>
      {p}
      </option>
    ))}
    </select>

    <label style={{ fontSize: 12, opacity: 0.8 }}>Top</label>
    <select value={top} onChange={(e) => setTop(Number(e.target.value))}>
    {[10, 25, 50, 100].map((n) => (
      <option key={n} value={n}>
      {n}
      </option>
    ))}
    </select>

    {product === "PIREP" ? (
      <>
      <label style={{ fontSize: 12, opacity: 0.8 }}>Hours</label>
      <select value={hours} onChange={(e) => setHours(Number(e.target.value))}>
      {[6, 12, 24, 36, 48, 72].map((h) => (
        <option key={h} value={h}>
        {h}
        </option>
      ))}
      </select>
      </>
    ) : null}

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
    <th style={{ padding: 8 }}>{product}</th>
    </tr>
    </thead>
    <tbody>
    {rows.map((r, i) => (
      <tr
      key={`${r.station}-${i}`}
      onClick={() => onRowClick(r, i)}
      style={{
        borderBottom: "1px solid #eee",
        verticalAlign: "top",
        cursor: product === "PIREP" ? "default" : "pointer",
        opacity: product === "PIREP" ? 0.98 : 1,
      }}
      title={product === "PIREP" ? "" : "Click for a shareable link"}
      >
      <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
      <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
      <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{formatScore(r.score)}</td>
      {/* backend now uses r.text */}
      <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{r.text}</td>
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
    <TileLayer attribution="&copy; OpenStreetMap contributors" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
    {markers.map((r, i) => (
      <Marker key={`${r.station}-${i}`} position={[r.lat, r.lon]}>
      <Popup>
      <div style={{ fontFamily: "monospace" }}>
      <div>
      <b>{r.station}</b> — score {formatScore(r.score)}
      </div>
      <div style={{ whiteSpace: "pre-wrap" }}>{r.text}</div>
      {product !== "PIREP" ? (
        <div style={{ marginTop: 8 }}>
        <Link to={`/station/${encodeURIComponent(r.station)}`}>Open link</Link>
        </div>
      ) : null}
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

  const [top] = useState(200); // fetch bigger list to increase odds of finding station
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [err, setErr] = useState("");

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const mapRef = useRef(null);

  // Station page: still pulls METAR leaderboard (since it’s station-based)
  useEffect(() => {
    (async () => {
      try {
        const data = await fetchProduct({ product: "METAR", top, hours: 24 });
        setRows(data.rows || []);
        setGeneratedAt(data.generated_at_utc || null);
      } catch (e) {
        setErr(String(e));
      }
    })();
  }, [top]);

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
    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, marginBottom: 12, flexWrap: "wrap" }}>
    <div>
    <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>Station: {icao}</div>
    <div style={{ fontSize: 12, opacity: 0.75 }}>
    {generatedAt ? `UTC: ${generatedAt}` : ""}
    {" • Shareable permalink page"}
    </div>
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
      <div>{row.text}</div>
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
    <TileLayer attribution="&copy; OpenStreetMap contributors" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
    {row && typeof row.lat === "number" && typeof row.lon === "number" ? (
      <Marker position={[row.lat, row.lon]}>
      <Popup>
      <div style={{ fontFamily: "monospace" }}>
      <b>{row.station}</b> — {formatScore(row.score)}
      <div style={{ whiteSpace: "pre-wrap" }}>{row.text}</div>
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
    {/* safety */}
    <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
