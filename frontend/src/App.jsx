import { useEffect, useMemo, useRef, useState } from "react";
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

  const r = await fetch(url);
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status} ${t}`.trim());
  }
  return r.json();
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

// Simple RainViewer radar tiles (public). If it ever changes, we can swap providers.
const RAINVIEWER_TILE =
"https://tilecache.rainviewer.com/v2/radar/{time}/256/{z}/{x}/{y}/2/1_1.png";

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

        const navigate = useNavigate();
        const mapRef = useRef(null);

        const mapCenter = useMemo(() => [39.5, -98.35], []);
        const markers = rows.filter(
          (r) => typeof r.lat === "number" && typeof r.lon === "number"
        );

        async function load() {
          setLoading(true);
          setErr("");
          try {
            const data = await fetchProduct({
              product,
              top,
              conus: conusOnly,
              hours: pirepHours,
            });
            setRows(data.rows || []);
            setGeneratedAt(data.generated_at_utc || null);
          } catch (e) {
            setErr(String(e));
            setRows([]);
            setGeneratedAt(null);
          } finally {
            setLoading(false);
          }
        }

        useEffect(() => {
          load();
          const t = setInterval(load, 60_000);
          return () => clearInterval(t);
          // eslint-disable-next-line react-hooks/exhaustive-deps
        }, [top, product, pirepHours, conusOnly]);

        function onRowClick(row) {
          // PIREPs don't have a meaningful station permalink in this app.
          if (row.product === "PIREP") return;
          const st = (row.station || "").trim();
          if (!st) return;
          navigate(`/station/${st}?product=${encodeURIComponent(row.product || product)}`);
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
            flexWrap: "wrap",
          }}
          >
          <div>
          <div style={{ fontSize: 18, fontWeight: 700 }}>
          Aviation Weather Leaderboard
          </div>
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
            <select
            value={pirepHours}
            onChange={(e) => setPirepHours(Number(e.target.value))}
            >
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
            <div style={{ color: "crimson", fontFamily: "monospace", whiteSpace: "pre-wrap" }}>
            {err}
            </div>
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
            const key = `${r.product || product}-${r.station || "----"}-${i}`;
            const clickable = (r.product || product) !== "PIREP";
            return (
              <tr
              key={key}
              onClick={() => (clickable ? onRowClick(r) : undefined)}
              style={{
                borderBottom: "1px solid #eee",
                verticalAlign: "top",
                cursor: clickable ? "pointer" : "default",
                opacity: clickable ? 1 : 0.95,
              }}
              title={clickable ? "Click for a shareable link" : "PIREPs have no station permalink"}
              >
              <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>{i + 1}</td>
              <td style={{ padding: 8, fontFamily: "monospace" }}>{r.station}</td>
              <td style={{ padding: 8, fontVariantNumeric: "tabular-nums" }}>
              {formatScore(r.score)}
              </td>
              <td style={{ padding: 8, fontFamily: "monospace", whiteSpace: "pre-wrap" }}>
              {r.text}
              </td>
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

          {radarOn ? (
            <TileLayer
            url={RAINVIEWER_TILE.replace("{time}", "latest")}
            opacity={0.55}
            zIndex={10}
            />
          ) : null}

          {markers.map((r, i) => (
            <Marker key={`${r.product || product}-${r.station}-${i}`} position={[r.lat, r.lon]}>
            <Popup>
            <div style={{ fontFamily: "monospace" }}>
            <div>
            <b>{r.station}</b> — {r.product} score {formatScore(r.score)}
            </div>
            <div style={{ whiteSpace: "pre-wrap" }}>{r.text}</div>
            {(r.product || product) !== "PIREP" ? (
              <div style={{ marginTop: 8 }}>
              <Link to={`/station/${r.station}?product=${encodeURIComponent(r.product || product)}`}>
              Open link
              </Link>
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

        const [rows, setRows] = useState([]);
        const [err, setErr] = useState("");

        const mapCenter = useMemo(() => [39.5, -98.35], []);
        const mapRef = useRef(null);

        // product from querystring (default METAR)
        const qs = new URLSearchParams(window.location.hash.split("?")[1] || window.location.search);
        const product = (qs.get("product") || "METAR").toUpperCase();

        useEffect(() => {
          (async () => {
            try {
              // Fetch a larger list so the station likely appears
              const data = await fetchProduct({
                product,
                top: 200,
                conus: true,
                hours: 24,
              });
              setRows(data.rows || []);
            } catch (e) {
              setErr(String(e));
            }
          })();
        }, [product]);

        const row = rows.find(
          (r) => (r.station || "").toUpperCase() === (icao || "").toUpperCase()
        );

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
          <div style={{ fontSize: 18, fontWeight: 700, fontFamily: "monospace" }}>
          {product} Station: {icao}
          </div>
          <div style={{ fontSize: 12, opacity: 0.75 }}>Shareable permalink page</div>
          </div>
          <div style={{ display: "flex", gap: 8 }}>
          <button onClick={() => navigate("/")}>Back</button>
          <button onClick={onCopy}>Copy link</button>
          </div>
          </div>

          {err ? <div style={{ color: "crimson", fontFamily: "monospace" }}>{err}</div> : null}

          {!row ? (
            <div style={{ fontFamily: "monospace" }}>
            Station not found in the current list. Try again later.
            </div>
          ) : (
            <div style={{ fontFamily: "monospace", whiteSpace: "pre-wrap" }}>
            <div>
            <b>Score:</b> {formatScore(row.score)}
            </div>
            <div style={{ marginTop: 8 }}>
            <b>Text:</b>
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
          <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {row && typeof row.lat === "number" && typeof row.lon === "number" ? (
            <Marker position={[row.lat, row.lon]}>
            <Popup>
            <div style={{ fontFamily: "monospace" }}>
            <b>{row.station}</b> — {product} {formatScore(row.score)}
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
          <Route path="/station/:icao" element={<StationPage />} />
          </Routes>
        );
      }
