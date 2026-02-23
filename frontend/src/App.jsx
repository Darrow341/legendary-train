import { useEffect, useMemo, useRef, useState, useCallback } from "react";
import { Routes, Route, useNavigate, useParams, Link } from "react-router-dom";
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
 * Normalize backend rows so the UI doesn't depend on one exact JSON shape.
 * We accept:
 *   - station/text
 *   - icaoId/rawOb/raw
 *   - lat/lon sometimes as strings
 */
function normalizeRow(r, fallbackProduct) {
  const product = (r?.product || fallbackProduct || "").toString();

  const station =
  (r?.station ?? r?.icaoId ?? r?.stationId ?? r?.station_id ?? "").toString().trim() || "----";

  const text =
  (r?.text ?? r?.rawOb ?? r?.rawTAF ?? r?.raw ?? r?.report ?? "").toString().trim();

  const score = typeof r?.score === "number" ? r.score : Number(r?.score);
  const lat = typeof r?.lat === "number" ? r.lat : r?.lat != null ? Number(r.lat) : null;
  const lon = typeof r?.lon === "number" ? r.lon : r?.lon != null ? Number(r.lon) : null;

  return {
    product,
    station,
    text,
    score: Number.isFinite(score) ? score : null,
    lat: Number.isFinite(lat) ? lat : null,
    lon: Number.isFinite(lon) ? lon : null,
  };
}

async function fetchJsonOrThrow(url) {
  const r = await fetch(url);
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`Fetch failed: ${url} -> HTTP ${r.status}${t ? `\n${t}` : ""}`);
  }
  return r.json();
}

async function fetchProduct({ product, top, conus, hours }) {
  let url = "";
  if (product === "METAR") {
    url = `/api/leaderboard?top=${top}&conus=${conus ? "true" : "false"}`;
  } else if (product === "TAF") {
    url = `/api/taf?top=${top}`;
  } else if (product === "PIREP") {
    url = `/api/pirep?top=${top}&hours=${hours}`;
  } else {
    throw new Error(`Unknown product: ${product}`);
  }
  return fetchJsonOrThrow(url);
}

/**
 * RainViewer radar tiles:
 * Get a current radar timestamp from their public API and build the tile URL.
 * Docs/behavior can change; this approach is the most reliable.
 */
async function fetchRainViewerLatestTimestamp() {
  // This endpoint returns: { radar: { past: [...], nowcast: [...] }, ... }
  // We'll prefer nowcast last frame, else past last frame.
  const url = "https://api.rainviewer.com/public/weather-maps.json";
  const data = await fetchJsonOrThrow(url);

  const nowcast = data?.radar?.nowcast;
  const past = data?.radar?.past;

  const pickLastTime = (arr) => {
    if (!Array.isArray(arr) || arr.length === 0) return null;
    const last = arr[arr.length - 1];
    const t = last?.time;
    return typeof t === "number" ? t : null;
  };

  return pickLastTime(nowcast) ?? pickLastTime(past);
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
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [radarTime, setRadarTime] = useState(null);
  const [radarErr, setRadarErr] = useState("");

  const navigate = useNavigate();
  const mapRef = useRef(null);

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const markers = useMemo(
    () => rows.filter((r) => typeof r.lat === "number" && typeof r.lon === "number"),
                          [rows]
  );

  const load = useCallback(async () => {
    setLoading(true);
    setErr("");
    try {
      const data = await fetchProduct({
        product,
        top,
        conus: conusOnly,
        hours: pirepHours,
      });

      const normalized = Array.isArray(data?.rows)
      ? data.rows.map((r) => normalizeRow(r, product)).filter((r) => r.text)
      : [];

      setRows(normalized);
      setGeneratedAt(data?.generated_at_utc || null);
    } catch (e) {
      setErr(String(e));
      setRows([]);
      setGeneratedAt(null);
    } finally {
      setLoading(false);
    }
  }, [product, top, conusOnly, pirepHours]);

  useEffect(() => {
    load();
    const t = setInterval(load, 60_000);
    return () => clearInterval(t);
  }, [load]);

  // Radar timestamp loading: only when radar is enabled (and refresh every 5 minutes)
  useEffect(() => {
    let alive = true;
    let timer = null;

    async function loadRadar() {
      setRadarErr("");
      try {
        const t = await fetchRainViewerLatestTimestamp();
        if (!alive) return;
        setRadarTime(t);
      } catch (e) {
        if (!alive) return;
        setRadarTime(null);
        setRadarErr(String(e));
      }
    }

    if (radarOn) {
      loadRadar();
      timer = setInterval(loadRadar, 5 * 60_000);
    } else {
      setRadarTime(null);
      setRadarErr("");
    }

    return () => {
      alive = false;
      if (timer) clearInterval(timer);
    };
  }, [radarOn]);

  function onRowClick(row) {
    const p = row.product || product;
    if (p === "PIREP") return;
    const st = (row.station || "").trim();
    if (!st || st === "----") return;
    navigate(`/station/${st}?product=${encodeURIComponent(p)}`);
  }

  const radarTileUrl = useMemo(() => {
    if (!radarTime) return null;
    // RainViewer tile pattern (256 tiles):
    // https://tilecache.rainviewer.com/v2/radar/{time}/256/{z}/{x}/{y}/2/1_1.png
    return `https://tilecache.rainviewer.com/v2/radar/${radarTime}/256/{z}/{x}/{y}/2/1_1.png`;
  }, [radarTime]);

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
    <div style={{ fontSize: 18, fontWeight: 700 }}>Aviation Weather Leaderboard</div>
    <div style={{ fontSize: 12, opacity: 0.75 }}>
    {generatedAt ? `UTC: ${generatedAt}` : ""}
    {loading ? " • loading…" : ""}
    </div>
    </div>

    <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
    <label style={{ fontSize: 12, opacity: 0.8 }}>Product</label>
    <select value={product} onChange={(e) => setProduct(e.target.value)}>
    <option value="METAR">METAR</option>
    <option value="TAF">TAF</option>
    <option value="PIREP">PIREP</option>
    </select>

    {product === "METAR" ? (
      <>
      <label style={{ fontSize: 12, opacity: 0.8 }}>CONUS</label>
      <input
      type="checkbox"
      checked={conusOnly}
      onChange={(e) => setConusOnly(e.target.checked)}
      title="Filter METARs to CONUS"
      />
      </>
    ) : null}

    {product === "PIREP" ? (
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
    {[10, 25, 50, 100].map((n) => (
      <option key={n} value={n}>
      {n}
      </option>
    ))}
    </select>

    <label style={{ fontSize: 12, opacity: 0.8 }}>Radar</label>
    <input
    type="checkbox"
    checked={radarOn}
    onChange={(e) => setRadarOn(e.target.checked)}
    title="Toggle radar overlay"
    />

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
    <th style={{ padding: 8 }}>Text</th>
    </tr>
    </thead>
    <tbody>
    {rows.map((r, i) => {
      const p = r.product || product;
      const key = `${p}-${r.station}-${i}`;
      const clickable = p !== "PIREP" && r.station !== "----";
      return (
        <tr
        key={key}
        onClick={() => (clickable ? onRowClick(r) : undefined)}
        style={{
          borderBottom: "1px solid #eee",
          verticalAlign: "top",
          cursor: clickable ? "pointer" : "default",
        }}
        title={clickable ? "Click for a shareable link" : "PIREPs have no station permalink"}
        >
        <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
        <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
        <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>
        {r.score != null ? formatScore(r.score) : ""}
        </td>
        <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>{r.text}</td>
        </tr>
      );
    })}
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
    <TileLayer
    attribution="&copy; OpenStreetMap contributors"
    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    />

    {radarOn && radarTileUrl ? <TileLayer url={radarTileUrl} opacity={0.55} zIndex={10} /> : null}

    {radarOn && radarErr ? (
      <Popup position={mapCenter}>
      <div style={{ fontFamily: "monospace", color: "crimson", whiteSpace: "pre-wrap" }}>
      Radar error:
      {"\n"}
      {radarErr}
      </div>
      </Popup>
    ) : null}

    {markers.map((r, i) => (
      <Marker key={`${r.product}-${r.station}-${i}`} position={[r.lat, r.lon]}>
      <Popup>
      <div style={{ fontFamily: "monospace" }}>
      <div style={{ fontWeight: 700, marginBottom: 6 }}>{r.station}</div>
      <div style={{ opacity: 0.8, marginBottom: 6 }}>
      {r.product} • score {r.score != null ? formatScore(r.score) : "—"}
      </div>
      <div style={{ whiteSpace: "pre-wrap" }}>{r.text}</div>
      <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
      <button
      onClick={() => copyText(r.text)}
      style={{ padding: "6px 10px", cursor: "pointer" }}
      title="Copy report text"
      >
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
        title="Copy shareable station link"
        >
        Copy link
        </button>
      ) : null}
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
  const { station } = useParams();
  const navigate = useNavigate();

  const [product, setProduct] = useState("METAR");
  const [top, setTop] = useState(25);

  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    // Pull product from query string if present
    const qs = new URLSearchParams(window.location.search);
    const p = qs.get("product");
    if (p === "METAR" || p === "TAF" || p === "PIREP") setProduct(p);
  }, []);

    const load = useCallback(async () => {
      setLoading(true);
      setErr("");
      try {
        // For station page: only METAR/TAF make sense; PIREP doesn't have station keying
        const p = product === "PIREP" ? "METAR" : product;

        const data = await fetchProduct({
          product: p,
          top,
          conus: false,
          hours: 24,
        });

        const normalized = Array.isArray(data?.rows)
        ? data.rows.map((r) => normalizeRow(r, p)).filter((r) => r.station === station)
        : [];

        setRows(normalized);
        setGeneratedAt(data?.generated_at_utc || null);
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
      {generatedAt ? `UTC: ${generatedAt}` : ""}
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
      {[10, 25, 50, 100].map((n) => (
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
        <th style={{ padding: 8 }}>Score</th>
        <th style={{ padding: 8 }}>Text</th>
        </tr>
        </thead>
        <tbody>
        {rows.map((r, i) => (
          <tr key={`${r.product}-${r.station}-${i}`} style={{ borderBottom: "1px solid #eee", verticalAlign: "top" }}>
          <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
          <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>
          {r.score != null ? formatScore(r.score) : ""}
          </td>
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
