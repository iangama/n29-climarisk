const fs = require("fs");
const crypto = require("crypto");
const { Pool } = require("pg");
const { Worker } = require("bullmq");
const promClient = require("prom-client");

function sha256Hex(s) {
  return crypto.createHash("sha256").update(s, "utf8").digest("hex");
}

function stableStringify(v) {
  if (v === null || v === undefined) return "null";
  if (typeof v === "number") return Number.isFinite(v) ? String(v) : "null";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "string") return JSON.stringify(v);
  if (Array.isArray(v)) return "[" + v.map(stableStringify).join(",") + "]";
  if (typeof v === "object") {
    if (typeof v.toJSON === "function") return stableStringify(v.toJSON());
    const keys = Object.keys(v).sort();
    return "{" + keys.map(k => JSON.stringify(k) + ":" + stableStringify(v[k])).join(",") + "}";
  }
  return "null";
}

function readKeyFromFile(path) {
  const raw = fs.readFileSync(path, "utf8").trim();
  if (!raw) throw new Error("OWM api key file empty");
  return raw;
}

// Regras explícitas
function decideRisk({ tempC, windMs, rain1hMm }) {
  const rules = [];

  if (rain1hMm >= 20) rules.push({ id: "RAIN_20MM_1H", severity: "CRITICAL", why: "rain_1h_mm>=20" });
  else if (rain1hMm >= 8) rules.push({ id: "RAIN_8MM_1H", severity: "ALERT", why: "rain_1h_mm>=8" });

  if (windMs >= 20) rules.push({ id: "WIND_20MS", severity: "CRITICAL", why: "wind_ms>=20" });
  else if (windMs >= 12) rules.push({ id: "WIND_12MS", severity: "ALERT", why: "wind_ms>=12" });

  if (tempC >= 38) rules.push({ id: "TEMP_38C", severity: "CRITICAL", why: "temp_c>=38" });
  else if (tempC >= 33) rules.push({ id: "TEMP_33C", severity: "ALERT", why: "temp_c>=33" });
  if (tempC <= -3) rules.push({ id: "TEMP_-3C", severity: "CRITICAL", why: "temp_c<=-3" });
  else if (tempC <= 2) rules.push({ id: "TEMP_2C", severity: "ALERT", why: "temp_c<=2" });

  let decision = "NORMAL";
  if (rules.some(r => r.severity === "CRITICAL")) decision = "CRITICAL";
  else if (rules.some(r => r.severity === "ALERT")) decision = "ALERT";

  rules.sort((a,b) => a.id.localeCompare(b.id));
  return { decision, applied_rule: { version: 1, inputs: { tempC, windMs, rain1hMm }, rules } };
}

async function fetchOpenWeather({ apiKey, lat, lon }) {
  const url = `https://api.openweathermap.org/data/2.5/weather?lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}&appid=${encodeURIComponent(apiKey)}&units=metric`;
  const r = await fetch(url, { method: "GET" });
  const text = await r.text();
  let json;
  try { json = JSON.parse(text); } catch { json = { raw: text }; }
  if (!r.ok) {
    const msg = (json && json.message) ? json.message : `owm_http_${r.status}`;
    throw new Error(msg);
  }
  return json;
}

const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "climarisk";
const OWM_API_KEY_FILE = process.env.OWM_API_KEY_FILE || "/run/secrets/owm_api_key";
const LOG_LEVEL = process.env.LOG_LEVEL || "info";

if (!DATABASE_URL) throw new Error("DATABASE_URL missing");
if (!REDIS_URL) throw new Error("REDIS_URL missing");

const pool = new Pool({ connectionString: DATABASE_URL });

promClient.collectDefaultMetrics();
const jobsProcessed = new promClient.Counter({
  name: "jobs_processed_total",
  help: "Total processed jobs",
  labelNames: ["name", "status"]
});

function log(...args) {
  if (LOG_LEVEL !== "silent") console.log("[worker]", ...args);
}

async function appendLedgerEvent(eventType, payloadObj) {
  const last = await pool.query("select event_hash from ledger_events order by id desc limit 1");
  const prevHash = last.rows[0].event_hash;

  const canonical = prevHash + "\n" + stableStringify({ event_type: eventType, payload: payloadObj });
  const eventHash = sha256Hex(canonical);

  await pool.query(
    "insert into ledger_events(event_type, payload, prev_hash, event_hash) values($1,$2,$3,$4)",
    [eventType, payloadObj, prevHash, eventHash]
  );

  return { eventHash };
}

async function applyProjections({ locationId, decision, applied_rule, raw_weather, ledger_hash }) {
  await pool.query(
    `insert into location_state(location_id, updated_at, decision, applied_rule, raw_weather, ledger_hash)
     values($1, now(), $2, $3, $4, $5)
     on conflict (location_id) do update
       set updated_at=excluded.updated_at,
           decision=excluded.decision,
           applied_rule=excluded.applied_rule,
           raw_weather=excluded.raw_weather,
           ledger_hash=excluded.ledger_hash`,
    [locationId, decision, applied_rule, raw_weather, ledger_hash]
  );

  await pool.query(
    `insert into weather_snapshots(location_id, decision, applied_rule, raw_weather, ledger_hash)
     values($1,$2,$3,$4,$5)`,
    [locationId, decision, applied_rule, raw_weather, ledger_hash]
  );
}

const worker = new Worker(
  QUEUE_NAME,
  async (job) => {
    if (job.name !== "refresh-location") return;

    const apiKey = readKeyFromFile(OWM_API_KEY_FILE);

    const { locationId } = job.data || {};
    if (!locationId) throw new Error("missing locationId");

    const lq = await pool.query("select id, name, lat, lon from locations where id=$1 and is_active=true", [locationId]);
    if (lq.rowCount === 0) throw new Error("location not found / inactive");

    const loc = lq.rows[0];
    log("consume job", job.id, "location", loc.id, loc.name);

    const raw = await fetchOpenWeather({ apiKey, lat: loc.lat, lon: loc.lon });

    const tempC = raw?.main?.temp ?? null;
    const windMs = raw?.wind?.speed ?? 0;
    const rain1hMm = raw?.rain?.["1h"] ?? 0;

    const pack = decideRisk({ tempC, windMs, rain1hMm });

    const payload = {
      location: { id: loc.id, name: loc.name, lat: loc.lat, lon: loc.lon },
      decision: pack.decision,
      applied_rule: pack.applied_rule,
      raw_weather: raw
    };

    const { eventHash } = await appendLedgerEvent("DECISION_WEATHER_RISK", payload);

    await applyProjections({
      locationId: loc.id,
      decision: pack.decision,
      applied_rule: pack.applied_rule,
      raw_weather: raw,
      ledger_hash: eventHash
    });

    return { ok: true, decision: pack.decision, ledger_hash: eventHash };
  },
  { connection: { url: REDIS_URL } }
);

worker.on("completed", (job, result) => {
  jobsProcessed.inc({ name: job.name, status: "ok" });
  log("completed", job.id, result?.decision, result?.ledger_hash);
});

worker.on("failed", (job, err) => {
  jobsProcessed.inc({ name: job?.name || "unknown", status: "fail" });
  log("failed", job?.id, err?.message);
});

log("started. queue=", QUEUE_NAME);
