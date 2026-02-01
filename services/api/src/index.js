const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const { Pool } = require("pg");
const { Queue } = require("bullmq");
const promClient = require("prom-client");
const { z } = require("zod");

function sha256Hex(s) {
  return crypto.createHash("sha256").update(s, "utf8").digest("hex");
}

// Serialização estável (determinística) + suporte a Date/toJSON
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

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL;
const QUEUE_NAME = process.env.QUEUE_NAME || "climarisk";
const OWM_API_KEY_FILE = process.env.OWM_API_KEY_FILE || "/run/secrets/owm_api_key";

if (!DATABASE_URL) throw new Error("DATABASE_URL missing");
if (!REDIS_URL) throw new Error("REDIS_URL missing");

const app = express();
app.use(express.json({ limit: "1mb" }));

const pool = new Pool({ connectionString: DATABASE_URL });
const queue = new Queue(QUEUE_NAME, { connection: { url: REDIS_URL } });

// Metrics
promClient.collectDefaultMetrics();
const httpRequests = new promClient.Counter({
  name: "http_requests_total",
  help: "Total HTTP requests",
  labelNames: ["method", "route", "status"]
});

app.use((req, res, next) => {
  res.on("finish", () => {
    const route = (req.route && req.route.path) ? req.route.path : req.path;
    httpRequests.inc({ method: req.method, route, status: String(res.statusCode) });
  });
  next();
});

app.get("/metrics", async (_req, res) => {
  res.set("Content-Type", promClient.register.contentType);
  res.send(await promClient.register.metrics());
});

app.get("/api/health", async (_req, res) => {
  const r = await pool.query("select 1 as ok");
  res.json({ ok: true, db: r.rows[0].ok === 1 });
});

// ---- Ledger append (imutável, hash-chain) ----
async function appendLedgerEvent(eventType, payloadObj) {
  const last = await pool.query("select event_hash from ledger_events order by id desc limit 1");
  const prevHash = last.rows[0].event_hash;

  const canonical = prevHash + "\n" + stableStringify({ event_type: eventType, payload: payloadObj });
  const eventHash = sha256Hex(canonical);

  await pool.query(
    "insert into ledger_events(event_type, payload, prev_hash, event_hash) values($1,$2,$3,$4)",
    [eventType, payloadObj, prevHash, eventHash]
  );

  return { prevHash, eventHash };
}

// ---- Commands ----
const LocationCreate = z.object({
  name: z.string().min(1).max(80),
  lat: z.number().min(-90).max(90),
  lon: z.number().min(-180).max(180)
});

app.post("/api/cmd/locations", async (req, res) => {
  const parsed = LocationCreate.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ ok: false, error: parsed.error.flatten() });

  const { name, lat, lon } = parsed.data;

  // NOTA: remove created_at do payload (era a fonte do mismatch)
  const q = await pool.query(
    "insert into locations(name, lat, lon) values($1,$2,$3) returning id, name, lat, lon, is_active",
    [name, lat, lon]
  );
  const loc = q.rows[0];

  await appendLedgerEvent("CMD_LOCATION_ADD", { location: loc });

  await queue.add("refresh-location", { locationId: loc.id }, { removeOnComplete: true, removeOnFail: 100 });

  res.json({ ok: true, location: loc });
});

app.post("/api/cmd/locations/:id/refresh", async (req, res) => {
  const id = req.params.id;
  const exists = await pool.query("select id from locations where id=$1 and is_active=true", [id]);
  if (exists.rowCount === 0) return res.status(404).json({ ok: false, error: "location not found" });

  await appendLedgerEvent("CMD_LOCATION_REFRESH", { locationId: id });

  const job = await queue.add("refresh-location", { locationId: id }, { removeOnComplete: true, removeOnFail: 100 });
  res.json({ ok: true, enqueued: true, jobId: job.id });
});

// ---- Reads (projeções) ----
app.get("/api/read/locations", async (_req, res) => {
  const q = await pool.query(
    `select l.id, l.name, l.lat, l.lon, l.is_active, l.created_at,
            s.updated_at, s.decision, s.applied_rule, s.raw_weather, s.ledger_hash
       from locations l
       left join location_state s on s.location_id = l.id
      where l.is_active=true
      order by l.created_at asc`
  );
  res.json({ ok: true, locations: q.rows });
});

// ---- Audit: verifica integridade do hash-chain ----
app.get("/api/read/audit/verify", async (_req, res) => {
  const q = await pool.query("select id, event_type, payload, prev_hash, event_hash from ledger_events order by id asc");
  const rows = q.rows;

  let ok = true;
  const errors = [];

  for (let i = 0; i < rows.length; i++) {
    const e = rows[i];

    const canonical = e.prev_hash + "\n" + stableStringify({ event_type: e.event_type, payload: e.payload });
    const computed = sha256Hex(canonical);

    if (computed !== e.event_hash) {
      ok = false;
      errors.push({ id: String(e.id), error: "hash_mismatch", expected: e.event_hash, got: computed });
    }

    if (i === 0) {
      if (e.prev_hash !== "0") {
        ok = false;
        errors.push({ id: String(e.id), error: "genesis_prev_hash_not_zero", prev_hash: e.prev_hash });
      }
    } else {
      const prev = rows[i - 1];
      if (e.prev_hash !== prev.event_hash) {
        ok = false;
        errors.push({ id: String(e.id), error: "prev_hash_link_broken", prev_hash: e.prev_hash, should_be: prev.event_hash });
      }
    }
  }

  res.json({ ok, count: rows.length, errors });
});

app.listen(PORT, () => {
  try { readKeyFromFile(OWM_API_KEY_FILE); } catch (e) { console.error("[api] owm key read failed:", e.message); }
  console.log(`[api] listening on :${PORT}`);
});
