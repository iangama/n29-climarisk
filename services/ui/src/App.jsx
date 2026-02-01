import React, { useEffect, useMemo, useState } from "react";

async function j(method, url, body) {
  const hasBody = body !== undefined && body !== null;
  const r = await fetch(url, {
    method,
    headers: hasBody ? { "content-type": "application/json" } : {},
    body: hasBody ? JSON.stringify(body) : undefined
  });
  const t = await r.text();
  let data;
  try { data = JSON.parse(t); } catch { data = { raw: t }; }
  if (!r.ok) throw new Error(data?.error || data?.message || `http_${r.status}`);
  return data;
}

function Badge({ decision }) {
  const s = decision || "—";
  return (
    <span style={{
      padding: "2px 8px",
      borderRadius: 999,
      border: "1px solid rgba(255,255,255,0.15)",
      fontSize: 12
    }}>
      {s}
    </span>
  );
}

export default function App() {
  const [locations, setLocations] = useState([]);
  const [audit, setAudit] = useState(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const [name, setName] = useState("Belo Horizonte");
  const [lat, setLat] = useState(-19.9167);
  const [lon, setLon] = useState(-43.9345);

  const total = useMemo(() => locations.length, [locations]);

  async function reload() {
    setErr("");
    setLoading(true);
    try {
      const r = await j("GET", "/api/read/locations");
      setLocations(r.locations || []);
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function runAudit() {
    setErr("");
    setLoading(true);
    try {
      const r = await j("GET", "/api/read/audit/verify");
      setAudit(r);
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function addLocation() {
    setErr("");
    setLoading(true);
    try {
      await j("POST", "/api/cmd/locations", { name, lat: Number(lat), lon: Number(lon) });
      await reload();
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function refresh(id) {
    setErr("");
    setLoading(true);
    try {
      await j("POST", `/api/cmd/locations/${id}/refresh`);
      // dá tempo do worker processar; recarrega depois
      setTimeout(() => reload(), 1200);
    } catch (e) {
      setErr(String(e.message || e));
      setLoading(false);
    }
  }

  useEffect(() => { reload(); }, []);

  return (
    <div style={{ minHeight: "100vh", background: "#0b0f14", color: "rgba(255,255,255,0.92)", fontFamily: "ui-sans-serif, system-ui" }}>
      <div style={{ maxWidth: 1100, margin: "0 auto", padding: 18 }}>
        <header style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
          <div>
            <div style={{ fontSize: 22, fontWeight: 800 }}>N29 ClimaRisk</div>
            <div style={{ opacity: 0.72, fontSize: 13 }}>Decisão climática real → ledger imutável → projeções → UI</div>
          </div>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <button onClick={reload} disabled={loading} style={btn()}>
              {loading ? "..." : "Recarregar"}
            </button>
            <button onClick={runAudit} disabled={loading} style={btn("ghost")}>
              Audit Verify
            </button>
          </div>
        </header>

        {err ? (
          <div style={{ marginTop: 12, padding: 12, borderRadius: 12, background: "rgba(255,0,0,0.10)", border: "1px solid rgba(255,0,0,0.25)" }}>
            {err}
          </div>
        ) : null}

        <section style={{ marginTop: 14, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <div style={card()}>
            <div style={{ fontWeight: 800, marginBottom: 8 }}>Adicionar Location</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 120px 120px", gap: 10 }}>
              <input value={name} onChange={(e) => setName(e.target.value)} placeholder="Nome" style={inp()} />
              <input value={lat} onChange={(e) => setLat(e.target.value)} placeholder="Lat" style={inp()} />
              <input value={lon} onChange={(e) => setLon(e.target.value)} placeholder="Lon" style={inp()} />
            </div>
            <div style={{ marginTop: 10, display: "flex", gap: 10 }}>
              <button onClick={addLocation} disabled={loading} style={btn()}>
                Add + Refresh
              </button>
              <div style={{ opacity: 0.7, fontSize: 12, alignSelf: "center" }}>API: /api/cmd/locations</div>
            </div>
          </div>

          <div style={card()}>
            <div style={{ fontWeight: 800, marginBottom: 8 }}>Audit status</div>
            <div style={{ opacity: 0.8, fontSize: 13 }}>
              {audit ? (
                <div style={{ display: "grid", gap: 6 }}>
                  <div>ok: <b>{String(audit.ok)}</b></div>
                  <div>count: <b>{audit.count}</b></div>
                  <div>errors: <b>{audit.errors?.length || 0}</b></div>
                  {audit.errors?.length ? (
                    <pre style={pre()}>{JSON.stringify(audit.errors.slice(0, 5), null, 2)}</pre>
                  ) : null}
                </div>
              ) : (
                <div>Rode “Audit Verify” para provar integridade do hash-chain.</div>
              )}
            </div>
          </div>
        </section>

        <section style={{ marginTop: 12 }}>
          <div style={card()}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 12 }}>
              <div style={{ fontWeight: 800 }}>Locations ({total})</div>
              <div style={{ opacity: 0.7, fontSize: 12 }}>Read model: /api/read/locations</div>
            </div>

            <div style={{ marginTop: 10, display: "grid", gap: 10 }}>
              {locations.map((l) => (
                <div key={l.id} style={row()}>
                  <div style={{ display: "grid", gap: 4 }}>
                    <div style={{ fontWeight: 800 }}>{l.name}</div>
                    <div style={{ opacity: 0.75, fontSize: 12 }}>
                      {Number(l.lat).toFixed(4)}, {Number(l.lon).toFixed(4)}
                    </div>
                  </div>

                  <div style={{ display: "grid", gap: 6, justifyItems: "end" }}>
                    <Badge decision={l.decision} />
                    <button onClick={() => refresh(l.id)} disabled={loading} style={btn("ghost")}>
                      Refresh
                    </button>
                  </div>

                  <div style={{ gridColumn: "1 / -1", opacity: 0.85 }}>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 8 }}>
                      <div>
                        <div style={label()}>hash do evento</div>
                        <div style={mono()}>{l.ledger_hash || "—"}</div>
                      </div>
                      <div>
                        <div style={label()}>regra aplicada</div>
                        <pre style={pre()}>{JSON.stringify(l.applied_rule || {}, null, 2)}</pre>
                      </div>
                      <div style={{ gridColumn: "1 / -1" }}>
                        <div style={label()}>dados brutos</div>
                        <pre style={pre()}>{JSON.stringify(l.raw_weather || {}, null, 2)}</pre>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
              {!locations.length ? (
                <div style={{ opacity: 0.75, padding: 10 }}>
                  Sem locations ainda. Adicione acima e o worker vai decidir de verdade.
                </div>
              ) : null}
            </div>
          </div>
        </section>

        <footer style={{ marginTop: 14, opacity: 0.65, fontSize: 12 }}>
          Observabilidade via Traefik: <code>/grafana</code>, <code>/prometheus</code>, <code>/loki</code> (porta 8880).
        </footer>
      </div>
    </div>
  );
}

function card() {
  return {
    borderRadius: 16,
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.10)",
    padding: 12
  };
}
function row() {
  return {
    borderRadius: 14,
    background: "rgba(0,0,0,0.25)",
    border: "1px solid rgba(255,255,255,0.10)",
    padding: 12,
    display: "grid",
    gridTemplateColumns: "1fr auto",
    gap: 10
  };
}
function btn(variant) {
  const base = {
    borderRadius: 12,
    padding: "10px 12px",
    border: "1px solid rgba(255,255,255,0.15)",
    cursor: "pointer",
    color: "rgba(255,255,255,0.92)"
  };
  if (variant === "ghost") {
    return { ...base, background: "transparent" };
  }
  return { ...base, background: "rgba(255,255,255,0.08)" };
}
function inp() {
  return {
    borderRadius: 12,
    padding: "10px 12px",
    border: "1px solid rgba(255,255,255,0.12)",
    background: "rgba(0,0,0,0.25)",
    color: "rgba(255,255,255,0.92)",
    outline: "none"
  };
}
function label() {
  return { fontSize: 12, opacity: 0.7, marginBottom: 4 };
}
function mono() {
  return { fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace", fontSize: 12, opacity: 0.9 };
}
function pre() {
  return {
    margin: 0,
    padding: 10,
    borderRadius: 12,
    border: "1px solid rgba(255,255,255,0.10)",
    background: "rgba(0,0,0,0.30)",
    overflow: "auto",
    maxHeight: 260,
    fontSize: 12
  };
}
