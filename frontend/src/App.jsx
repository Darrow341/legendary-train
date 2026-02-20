import { useEffect, useMemo, useState } from "react";
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

export default function App() {
  const [top, setTop] = useState(25);
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  async function load() {
    setLoading(true);
    setErr("");
    try {
      const r = await fetch(`/api/leaderboard?top=${top}&conus=true`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setRows(data.rows || []);
      setGeneratedAt(data.generated_at_utc || null);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const t = setInterval(load, 60000);
    return () => clearInterval(t);
  }, [top]);

  const mapCenter = useMemo(() => [39.5, -98.35], []);
  const markers = rows.filter(
    (r) => typeof r.lat === "number" && typeof r.lon === "number"
  );

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
      {/* Leaderboard */}
      <div
        style={{
          overflow: "auto",
          border: "1px solid #ddd",
          borderRadius: 12,
          padding: 12,
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 12,
          }}
        >
          <div>
            <div style={{ fontSize: 18, fontWeight: 700 }}>
              METAR Leaderboard (CONUS)
            </div>

            <div style={{ fontSize: 12, opacity: 0.75 }}>
              {generatedAt ? `UTC: ${generatedAt}` : ""}
              {loading ? " • loading…" : ""}
            </div>
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <label>Top</label>

            <select
              value={top}
              onChange={(e) => setTop(Number(e.target.value))}
            >
              {[10, 25, 50, 100].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>

            <button onClick={load}>Refresh</button>
          </div>
        </div>

        {err && (
          <div
            style={{
              color: "crimson",
              fontFamily: "monospace",
              marginBottom: 12,
            }}
          >
            {err}
          </div>
        )}

        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #ddd" }}>
              <th>#</th>
              <th>Station</th>
              <th>Score</th>
              <th>METAR</th>
            </tr>
          </thead>

          <tbody>
            {rows.map((r, i) => (
              <tr
                key={`${r.station}-${i}`}
                style={{ borderBottom: "1px solid #eee" }}
              >
                <td>{i + 1}</td>

                <td style={{ fontFamily: "monospace" }}>{r.station}</td>

                <td>{formatScore(r.score)}</td>

                <td
                  style={{
                    fontFamily: "monospace",
                    whiteSpace: "pre-wrap",
                  }}
                >
                  {r.metar}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Map */}
      <div
        style={{
          border: "1px solid #ddd",
          borderRadius: 12,
          overflow: "hidden",
        }}
      >
        <MapContainer
          center={mapCenter}
          zoom={4}
          style={{ height: "100%", width: "100%" }}
        >
          <TileLayer
            attribution="© OpenStreetMap contributors"
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          {markers.map((r, i) => (
            <Marker
              key={`${r.station}-${i}`}
              position={[r.lat, r.lon]}
            >
              <Popup>
                <div style={{ fontFamily: "monospace" }}>
                  <b>{r.station}</b>
                  <br />
                  Score: {formatScore(r.score)}
                  <br />
                  {r.metar}
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
}
