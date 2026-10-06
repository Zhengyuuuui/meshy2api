#!/usr/bin/env node
// meshy2api-gateway — single-account Meshy.ai gateway
// OpenAI-compatible chat / image / 3D / animation bridge. Zero-dependency Node 18+.
//
// This is the *single-account* fork of meshy2api: the multi-account pool,
// batch registration, credit tasks, proxy pool and IP risk guard have all been
// removed. One Meshy account (access/refresh token + browser device id) is
// configured via env or config.json → gateway. See README.md.

import http from "node:http";
import https from "node:https";
import net from "node:net";
import tls from "node:tls";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { decryptMeshy, isMeshyEncrypted } from "./recon/decrypt/meshy_decrypt.mjs";

// ---------- config ----------

const args = process.argv.slice(2);
function argOf(name, fallback) {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
}

const CONFIG_PATH = path.resolve(argOf("config", "config.json"));
const ROOT_DIR = path.dirname(fileURLToPath(import.meta.url));
let config = {};
try { config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8")); }
catch (e) {
  console.error(`[meshy2api] cannot read config ${CONFIG_PATH}: ${e.message}`);
  console.error(`[meshy2api] copy config.example.json → config.json and fill gateway.deviceId + gateway.refreshToken`);
  process.exit(1);
}
const PORT = Number(argOf("port", config.port ?? 8090));
const HOST = argOf("host", config.host ?? "127.0.0.1");
const BRIDGE_STARTED_AT = Date.now();
const BRIDGE_API_KEY = process.env.MESHY2API_KEY || config.apiKey || "";

const SUPABASE_AUTH_URL = (config.supabase?.authUrl ?? "https://auth.meshy.ai").replace(/\/$/, "");
const SUPABASE_ANON_KEY = config.supabase?.anonKey ?? "";

const ORIGIN = "https://www.meshy.ai";
const AGENT = `${ORIGIN}/agent-api/web`;
const MESHYD = `${ORIGIN}/meshyd-api/web`;
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36";

const D = {
  imageTurnSeconds: config.timeouts?.imageTurnSeconds ?? 240,
  model3dTurnSeconds: config.timeouts?.model3dTurnSeconds ?? 600,
  pollIntervalSeconds: config.timeouts?.pollIntervalSeconds ?? 3,
};
const DEFAULTS = {
  imageModel: config.defaults?.imageModel ?? "nano-banana-2-lite",
  model3d: config.defaults?.model3d ?? "meshy-7",
  aspectRatio: config.defaults?.aspectRatio ?? "1:1",
};
const DATA_DIR = path.resolve(config.dataDir ?? path.join(path.dirname(CONFIG_PATH), "data"));
fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------- upstream proxy (zero-dependency HTTP CONNECT tunnel) ----------
// Node's global fetch ignores HTTP(S)_PROXY, and this project has no deps, so we
// tunnel https requests through a local proxy (e.g. Clash 127.0.0.1:7890) using
// node:https over a CONNECT socket — same response shape as fetch.
// config.json:  { "proxy": "http://127.0.0.1:7890" }
// env:          MESHY_PROXY / HTTPS_PROXY / HTTP_PROXY / ALL_PROXY
const PROXY_URL = (() => {
  const raw = process.env.MESHY_PROXY || config.proxy ||
    process.env.HTTPS_PROXY || process.env.https_proxy ||
    process.env.HTTP_PROXY || process.env.http_proxy ||
    process.env.ALL_PROXY || "";
  if (!raw) return null;
  try {
    const u = new URL(raw.includes("://") ? raw : `http://${raw}`);
    if (!u.hostname) return null;
    return { host: u.hostname, port: Number(u.port) || 80 };
  } catch { return null; }
})();

/** Open a CONNECT tunnel to targetHost:targetPort through the proxy. */
function upstreamConnect({ host, port }, targetHost, targetPort, timeoutMs = 15_000) {
  return new Promise((resolve, reject) => {
    const req = http.request({
      host, port, method: "CONNECT", path: `${targetHost}:${targetPort}`,
      headers: { Host: `${targetHost}:${targetPort}` }, timeout: timeoutMs,
    });
    req.once("connect", (res, socket) => {
      if (res.statusCode !== 200) { socket.destroy(); reject(new Error(`proxy CONNECT ${targetHost}:${targetPort} -> ${res.statusCode}`)); return; }
      resolve(socket);
    });
    req.once("error", reject);
    req.once("timeout", () => req.destroy(new Error("proxy CONNECT timeout")));
    req.end();
  });
}

/**
 * Proxy-aware http(s) request. Mirrors the subset of fetch() we use:
 *   { status, ok, headers:{get}, text(), json(), arrayBuffer(), [body] }
 *
 * Three behaviours that MUST NOT regress (all previously bit us):
 *   1. manual redirect following (signed CDN 302 on GLB / image downloads)
 *   2. `text/event-stream` responses returned as a real ReadableStream (agent turn)
 *   3. `ok` + `arrayBuffer()` present (GLB decryption / !res.ok checks)
 */
async function upstreamFetch(url, { method = "GET", headers = {}, body, timeoutMs = 30_000, redirect } = {}) {
  const noProxy = !PROXY_URL || new URL(url).protocol !== "https:";
  if (noProxy) {
    const ac = new AbortController();
    const t = setTimeout(() => ac.abort(), timeoutMs);
    try {
      const r = await fetch(url, { method, headers, body, redirect, signal: ac.signal });
      const ct = r.headers.get("content-type") ?? "";
      const hget = { get: (k) => r.headers.get(k) };
      // streaming: expose the live body as a ReadableStream, matching the proxy path
      if (ct.includes("text/event-stream") || ct.includes("application/x-ndjson")) {
        clearTimeout(t);
        const stream = r.body ?? (() => {
          throw new Error("upstream response has no body stream");
        })();
        const drain = async () => {
          const rd = stream.getReader(); const cs = [];
          for (;;) { const { done, value } = await rd.read(); if (done) break; cs.push(Buffer.from(value)); }
          return Buffer.concat(cs);
        };
        return {
          status: r.status,
          ok: r.ok,
          headers: hget,
          body: stream,
          arrayBuffer: drain,
          text: async () => (await drain()).toString("utf8"),
          json: async () => null,
        };
      }
      const buf = Buffer.from(await r.arrayBuffer());
      const text = buf.toString("utf8");
      return {
        status: r.status,
        ok: r.ok,
        headers: hget,
        text: async () => text,
        arrayBuffer: async () => buf,
        json: async () => { try { return JSON.parse(text); } catch { return null; } },
      };
    } finally { clearTimeout(t); }
  }

  // proxy path: follow redirects manually (signed CDN links 302), max 5 hops
  const MAX_HOPS = 5;
  let cur = new URL(url);
  let curMethod = method, curBody = body, curHeaders = { ...headers };

  for (let hop = 0; hop <= MAX_HOPS; hop++) {
    const sock = await upstreamConnect(PROXY_URL, cur.hostname, Number(cur.port) || 443, timeoutMs);
    let outcome;
    try {
      outcome = await new Promise((resolve, reject) => {
        const req = https.request({
          host: cur.hostname,
          port: Number(cur.port) || 443,
          path: cur.pathname + cur.search,
          method: curMethod,
          headers: { ...curHeaders, host: cur.host },
          timeout: timeoutMs,
          createConnection: () => tls.connect({ socket: sock, servername: cur.hostname, rejectUnauthorized: false }),
        }, (res) => {
          const code = res.statusCode;
          const loc = res.headers.location;
          if ([301, 302, 303, 307, 308].includes(code) && loc) {
            res.resume();
            resolve({ redirect: new URL(loc, cur).toString(), status: code });
            return;
          }
          const ct = res.headers["content-type"] ?? "";
          // SSE / streaming: return a real ReadableStream (agent turn depends on it)
          if (ct.includes("text/event-stream") || ct.includes("application/x-ndjson")) {
            const rs = new ReadableStream({
              start(controller) {
                res.on("data", (c) => controller.enqueue(new Uint8Array(c)));
                res.on("end", () => { try { controller.close(); } catch {} });
                res.on("error", (e) => { try { controller.error(e); } catch {} });
              },
            });
            const drain = async () => {
              const rd = rs.getReader(); const cs = [];
              for (;;) { const { done, value } = await rd.read(); if (done) break; cs.push(Buffer.from(value)); }
              return Buffer.concat(cs);
            };
            resolve({
              status: code,
              ok: code >= 200 && code < 300,
              headers: { get: (k) => res.headers[String(k).toLowerCase()] ?? null },
              body: rs,
              arrayBuffer: drain,
              text: async () => (await drain()).toString("utf8"),
              json: async () => null,
            });
            return;
          }
          const chunks = [];
          res.on("data", (c) => chunks.push(c));
          res.on("end", () => {
            const buf = Buffer.concat(chunks);
            resolve({
              status: code,
              ok: code >= 200 && code < 300,
              headers: { get: (k) => res.headers[String(k).toLowerCase()] ?? null },
              text: async () => buf.toString("utf8"),
              arrayBuffer: async () => buf,
              json: async () => { try { return JSON.parse(buf.toString("utf8")); } catch { return null; } },
            });
          });
        });
        req.once("error", reject);
        req.once("timeout", () => req.destroy(new Error("upstream timeout")));
        if (curBody != null) req.write(typeof curBody === "string" || Buffer.isBuffer(curBody) ? curBody : JSON.stringify(curBody));
        req.end();
      });
    } finally {
      // SSE sockets are owned by the stream; recycle everything else here
      if (!outcome?.body) sock.destroy();
    }

    if (outcome?.redirect) {
      const next = new URL(outcome.redirect);
      if (hop < MAX_HOPS) {
        cur = next;
        if (outcome.status === 303 || ((outcome.status === 301 || outcome.status === 302) && curMethod === "POST")) {
          curMethod = "GET"; curBody = null;
          const { "Content-Type": _c, "Content-Length": _l, ...rest } = curHeaders;
          curHeaders = rest;
        }
        continue;
      }
    }
    return outcome;
  }
  throw new Error(`too many redirects: ${url}`);
}

// Retry transient network failures (ECONNRESET / TLS RST / socket hang up).
const sleep = (ms) => new Promise((s) => setTimeout(s, ms));
async function withRetry(fn, { retries = 2, baseMs = 300, label = "fetch" } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      const transient = e?.cause?.code === "ECONNRESET" || e?.cause?.code === "ECONNREFUSED" ||
        e?.cause?.code === "ETIMEDOUT" || e?.cause?.code === "EPIPE" || e?.cause?.code === "UND_ERR_SOCKET" ||
        e?.cause?.code === "CERT_HAS_EXPIRED" || /fetch failed|socket|network/i.test(String(e));
      if (!transient || attempt === retries) throw e;
      const waitMs = baseMs * 2 ** attempt;
      log(`retry ${label} #${attempt + 1} after ${waitMs}ms (${e?.cause?.code ?? String(e).slice(0, 60)})`);
      await sleep(waitMs);
    }
  }
  throw lastErr;
}

/**
 * Single network entry point: auth + proxy + retry in one place.
 * Returns the upstreamFetch/Response shape. Pass `noAuth` for public URLs
 * (CDN image/glb downloads, supabase token refresh).
 */
async function gwFetch(pathname, { method = "GET", body, headers: extra, timeoutMs = 60_000, noAuth = false, retries, redirect, signal } = {}) {
  const url = /^https?:\/\//i.test(pathname) ? pathname : `${ORIGIN}${pathname}`;
  const headers = noAuth
    ? { "User-Agent": UA, ...(extra ?? {}) }
    : { ...(await gw.headers()), "User-Agent": UA, ...(extra ?? {}) };
  let payload = body;
  if (body != null && !(body instanceof FormData) && !Buffer.isBuffer(body) && typeof body !== "string") {
    payload = JSON.stringify(body);
    if (!Object.keys(headers).some((k) => k.toLowerCase() === "content-type")) headers["Content-Type"] = "application/json";
  }
  const doFetch = body instanceof FormData
    ? () => fetch(url, { method, headers, body, signal: signal ?? AbortSignal.timeout(timeoutMs) })
    : () => upstreamFetch(url, { method, headers, body: payload, timeoutMs, redirect });
  // Only idempotent methods retry by default — replaying a POST/PUT could
  // double-create an upstream task. Callers may still force `retries` explicitly
  // (e.g. the agent turn POST relies on its server-side idempotency_key).
  const idempotent = method === "GET" || method === "HEAD" || method === "DELETE";
  const retryCount = retries ?? (idempotent ? 2 : 0);
  return withRetry(doFetch, { retries: retryCount, label: `${method} ${url.replace(ORIGIN, "").slice(0, 70)}` });
}

/** Read an upstream response into { status, ok, contentType, text, json }. */
async function readJson(res) {
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  return { status: res.status, ok: res.ok, contentType: res.headers.get("content-type") ?? "", text, json };
}
const gwJson = (pathname, opts) => gwFetch(pathname, opts).then(readJson);

// ---------- 3D model table ----------
// pipeline: image_to_3d_pipeline preference value (closed enum)
// aiModel : args.draft.aiModel real model id (null → agent-turn fallback path)
// display : official name
// cost    : upstream image_to_3d credits (with texture)
const MODEL3D_MAP = {
  // cost: no-texture / with-texture (upstream image_to_3d: should_texture false=20, true=30)
  "meshy-7":      { pipeline: "image_to_3d_meshy_7", aiModel: "blueberry",    display: "Meshy 7.1 - Flagship", cost: 30, costNoTex: 20 },
  "meshy-6":      { pipeline: "image_to_3d_meshy_6", aiModel: "avocado",      display: "Meshy 6",              cost: 30, costNoTex: 20 },
  "meshy-6-lite": { pipeline: "image_to_3d_meshy_6", aiModel: "meshy-6-lite", display: "Meshy 6 Lite",         cost: 30, costNoTex: 20 },
  "meshy-5.1":    { pipeline: "image_to_3d_meshy_6", aiModel: "meshy-5.1",    display: "Meshy 5",              cost: 30, costNoTex: 20 },
  "meshy-4":      { pipeline: "image_to_3d_meshy_6", aiModel: "meshy-4",      display: "Meshy 4",              cost: 30, costNoTex: 20 },
  "meshy-t2":     { pipeline: "smart_topology",       aiModel: null,           display: "Meshy T2 (smart topology)", cost: 5, costNoTex: 5 },
};
const MODEL3D_ALIASES = { blueberry: "meshy-7", avocado: "meshy-6" };
const resolve3dModel = (m) => MODEL3D_MAP[MODEL3D_ALIASES[m] ?? m] ?? null;

const IMAGE_MODELS = new Set(["nano-banana-2-lite", "nano-banana-2", "nano-banana-pro", "gpt-image-2", "gpt-image-2-5-flare", "gpt-image-2-5-sunburst"]);
const imageDataUrlRe = /^data:image\//;

// Official UI spendable balance: freeCreditBalance + creditBalance + shareCreditEarned + rolloverBalance.
function totalCredits(c) {
  if (!c) return null;
  return (Number(c.freeCreditBalance) || 0) + (Number(c.creditBalance) || 0) +
    (Number(c.shareCreditEarned) || 0) + (Number(c.rolloverBalance) || 0);
}

const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
const nanoid = (n = 21) => Array.from(crypto.randomFillSync(new Uint8Array(n))).map((b) => ALPHABET[b % 64]).join("");

function log(...xs) { console.log(new Date().toISOString(), ...xs); }

// ---------- credentials store (separate DB, safe to ship with the gateway) ----------
// Accounts live in their own SQLite file (config.credentialsDb ?? data/credentials.db)
// so the credential pool is kept apart from the operational DB (jobs3d / image_log).
// schema mirrors the original project's `accounts` table for easy import.

const CRED_DB_FILE = path.resolve(config.credentialsDb ?? path.join(DATA_DIR, "credentials.db"));
const credDb = new DatabaseSync(CRED_DB_FILE);
credDb.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS accounts (
    email TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    created_at INTEGER
  );
  CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
  );
`);
const credWarn = (label, e) => log(`credentials.db ${label} failed: ${e.message}`);

const credStore = {
  all() {
    try {
      return credDb.prepare("SELECT data FROM accounts ORDER BY created_at DESC").all()
        .map((r) => { try { return JSON.parse(r.data); } catch { return null; } }).filter(Boolean);
    } catch (e) { credWarn("all", e); return []; }
  },
  get(email) {
    try {
      const r = credDb.prepare("SELECT data FROM accounts WHERE email = ?").get(email);
      return r ? JSON.parse(r.data) : null;
    } catch (e) { credWarn("get", e); return null; }
  },
  upsert(rec) {
    try {
      credDb.prepare("INSERT INTO accounts (email, data, created_at) VALUES (?,?,?) ON CONFLICT(email) DO UPDATE SET data=excluded.data, created_at=excluded.created_at")
        .run(rec.email, JSON.stringify(rec), rec.created_at ?? Math.floor(Date.now() / 1000));
    } catch (e) { credWarn("upsert", e); }
  },
  remove(email) {
    try { credDb.prepare("DELETE FROM accounts WHERE email = ?").run(email); } catch (e) { credWarn("remove", e); }
  },
  activeEmail() {
    try { return credDb.prepare("SELECT value FROM meta WHERE key = 'active_email'").get()?.value ?? null; }
    catch { return null; }
  },
  setActiveEmail(email) {
    try {
      credDb.prepare("INSERT INTO meta (key, value) VALUES ('active_email', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(email);
    } catch (e) { credWarn("setActive", e); }
  },
};

// ---------- credentials: env > config.gateway > credentials.db active > data/session.json ----------

const SESSION_FILE = path.join(DATA_DIR, "session.json");

function loadSessionFile() {
  try {
    if (!fs.existsSync(SESSION_FILE)) return null;
    const s = JSON.parse(fs.readFileSync(SESSION_FILE, "utf8"));
    return s?.refresh_token || s?.access_token ? s : null;
  } catch { return null; }
}

const gatewayCfg = (() => {
  const g = config.gateway ?? {};
  const persisted = loadSessionFile();
  // credentials.db "active" account is the primary store; config.json is a fallback
  const active = credStore.activeEmail() ? credStore.get(credStore.activeEmail()) : null;
  const accessToken = process.env.MESHY_ACCESS_TOKEN || active?.access_token || g.accessToken || persisted?.access_token || "";
  const refreshToken = process.env.MESHY_REFRESH_TOKEN || active?.refresh_token || g.refreshToken || persisted?.refresh_token || "";
  const deviceId = process.env.MESHY_DEVICE_ID || active?.device_id || g.deviceId || persisted?.device_id || "";
  const email = process.env.MESHY_EMAIL || active?.email || g.email || persisted?.email || "";
  const expiresAt = active?.expires_at ?? persisted?.expires_at ?? 0;
  return { accessToken, refreshToken, deviceId, email, expiresAt };
})();

// When no credentials are configured we still start (so the console / browser-login
// endpoint is reachable) but run in "unconfigured" mode: generation endpoints will
// return 503 until an account is imported and activated.
const UNCONFIGURED = !gatewayCfg.deviceId || (!gatewayCfg.accessToken && !gatewayCfg.refreshToken);
if (UNCONFIGURED) {
  console.error("[meshy2api] starting WITHOUT credentials — only /health, /console and");
  console.error("  /v1/gateway/* account management are usable until you import an account.");
  console.error("  (browser login captures the session automatically once you log in with the button)");
}

// ---------- GatewayClient (single account) ----------

class GatewayClient {
  constructor(cfg) {
    this.accessToken = cfg.accessToken || null;
    this.refreshToken = cfg.refreshToken || null;
    this.deviceId = cfg.deviceId;
    this.email = cfg.email || "";
    this.expiresAt = cfg.expiresAt || 0;
    this.user = null;
    this.freeCredits = null;
    this.creditsBreakdown = null;
    this.refillAt = null;
    this.monthlyCredits = null;
    this.tierInfo = null;
    this.rigQuota = null;
    this.projectId = null;
    this.prefsReady = false;
    this._refreshing = null;
  }

  /** Hot-swap the live credentials (used by account import / switch, no restart needed). */
  loadFrom(rec) {
    this.accessToken = rec.access_token || null;
    this.refreshToken = rec.refresh_token || null;
    this.deviceId = rec.device_id || "";
    this.email = rec.email || "";
    this.expiresAt = rec.expires_at || 0;
    this.user = rec.user ?? null;
    this.freeCredits = rec.free_credits ?? null;
    this.creditsBreakdown = rec.credits_breakdown ?? null;
    this.refillAt = rec.refill_at ?? null;
    this.monthlyCredits = rec.monthly_credits ?? null;
    this.tierInfo = null;
    this.rigQuota = null;
    // project/prefs are account-scoped → force re-creation on next use
    this.projectId = null;
    this.prefsReady = false;
    this._refreshing = null;
    return this;
  }

  persist() {
    try {
      fs.writeFileSync(SESSION_FILE, JSON.stringify({
        access_token: this.accessToken,
        refresh_token: this.refreshToken,
        expires_at: this.expiresAt,
        expires_in: Math.max(0, this.expiresAt - Math.floor(Date.now() / 1000)),
        user: this.user,
        email: this.email,
        device_id: this.deviceId,
      }, null, 1));
      fs.chmodSync(SESSION_FILE, 0o600);
    } catch (e) { log(`persist session failed: ${e}`); }
  }

  async refresh() {
    if (!this.refreshToken) throw new Error("no refresh_token configured");
    const res = await gwFetch(`${SUPABASE_AUTH_URL}/auth/v1/token?grant_type=refresh_token`, {
      method: "POST", noAuth: true, retries: 1,
      headers: { "Content-Type": "application/json", apikey: SUPABASE_ANON_KEY, Authorization: `Bearer ${SUPABASE_ANON_KEY}` },
      body: JSON.stringify({ refresh_token: this.refreshToken }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data?.access_token) {
      const code = data?.error ?? data?.code;
      const msg = data?.error_description ?? data?.msg ?? `http ${res.status}`;
      const err = new Error(`refresh failed (${code ?? res.status}): ${msg}`);
      err.code = code ?? "refresh_failed";
      throw err;
    }
    this.accessToken = data.access_token;
    this.refreshToken = data.refresh_token ?? this.refreshToken;
    this.expiresAt = data.expires_at ?? Math.floor(Date.now() / 1000) + (data.expires_in ?? 900);
    this.user = data.user ?? this.user;
    if (!this.email && this.user?.email) this.email = this.user.email;
    this.persist();
    log(`token refreshed, new expiresAt=${this.expiresAt}`);
    return this.accessToken;
  }

  async getToken() {
    const now = Math.floor(Date.now() / 1000);
    if (this.accessToken && this.expiresAt - now > 60) return this.accessToken;
    if (!this._refreshing) {
      this._refreshing = this.refresh().finally(() => { this._refreshing = null; });
    }
    try {
      return await this._refreshing;
    } catch (e) {
      // current account's token is dead (401 / rotated elsewhere) → auto-replace
      // with another non-dead account from credentials.db, then retry once.
      if (isDeadTokenError(e) && this.refreshToken) {
        const oldEmail = this.email;
        markAccountDead(oldEmail, e?.message);
        const swapped = autoReplaceAccount(oldEmail);
        if (swapped) {
          log(`[auto-pool] active account dead → switched ${oldEmail || "?"} → ${swapped}`);
          return await this.getToken();
        }
      }
      throw e;
    }
  }

  async headers() {
    const token = await this.getToken();
    return {
      Authorization: `Bearer ${token}`,
      "X-Device-Id": this.deviceId,
      "X-Agent-Protocol-Version": "2",
      "x-locale": "zh",
      "Content-Type": "application/json",
    };
  }

  /** Authenticated upstream request. Returns { status, ok, contentType, text, json } (or raw res with stream:true). */
  async fetch(pathname, { method = "GET", body, timeoutMs = 60_000, headers: extraHeaders, retries, raw } = {}) {
    const token = await this.getToken();
    const url = pathname.startsWith("http") ? pathname : `${ORIGIN}${pathname}`;
    const baseHeaders = {
      Authorization: `Bearer ${token}`,
      "X-Device-Id": this.deviceId,
      "X-Agent-Protocol-Version": "2",
      "x-locale": "zh",
      "User-Agent": UA,
      ...(extraHeaders ?? {}),
    };
    const isForm = body instanceof FormData;
    let payload = body;
    if (body != null && !isForm && !Buffer.isBuffer(body) && typeof body !== "string") payload = JSON.stringify(body);
    if (!isForm && body != null && !Object.keys(baseHeaders).some((k) => k.toLowerCase() === "content-type")) {
      baseHeaders["Content-Type"] = "application/json";
    }
    const idempotent = method === "GET" || method === "HEAD" || method === "DELETE";
    const res = await withRetry(
      () => (isForm
        ? fetch(url, { method, headers: baseHeaders, body, signal: AbortSignal.timeout(timeoutMs) })
        : upstreamFetch(url, { method, headers: baseHeaders, body: payload, timeoutMs })),
      { label: `${method} ${pathname.slice(0, 60)}`, retries: retries ?? (idempotent ? 2 : 0) },
    );
    if (raw) return res;
    return readJson(res);
  }

  async runTurn({ projectId, content, timeoutSeconds, onEvent }) {
    return withTurnLock(() => this.runTurnLocked({ projectId, content, timeoutSeconds, onEvent }));
  }

  async runTurnLocked({ projectId, content, timeoutSeconds, onEvent }) {
    const chatId = nanoid();
    const body = { project_id: projectId, idempotency_key: `user-msg_${nanoid()}`, content };
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort("timeout"), timeoutSeconds * 1000);
    const events = [];
    try {
      const token = await this.getToken();
      const res = await gwFetch(`${AGENT}/v4/agent/threads/${chatId}/turns`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "X-Device-Id": this.deviceId, "X-Agent-Protocol-Version": "2", "x-locale": "zh",
          "Content-Type": "application/json", Accept: "text/event-stream" },
        body: JSON.stringify(body),
        // proxy path ignores AbortSignal → give the socket an inactivity timeout > turn budget
        timeoutMs: timeoutSeconds * 1000 + 5_000,
        signal: ac.signal,
        retries: 2,
      });
      const ct = res.headers.get("content-type") ?? "";
      if (ct.startsWith("application/json")) {
        const j = await res.json().catch(() => null);
        const text = j?.result ?? j?.message ?? j?.error?.string ?? "";
        return { events: [], finish: null, error: { code: j?.code ?? `http_${res.status}`, text: typeof text === "string" ? text : JSON.stringify(text) }, httpStatus: res.status };
      }
      if (!res.ok) {
        return { events: [], finish: null, error: { code: `http_${res.status}`, text: (await res.text()).slice(0, 300) }, httpStatus: res.status };
      }
      const reader = res.body.getReader();
      const dec = new TextDecoder();
      let buf = "";
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf("\n")) >= 0) {
          let line = buf.slice(0, idx);
          buf = buf.slice(idx + 1);
          if (line.endsWith("\r")) line = line.slice(0, -1);
          if (line.startsWith("data:")) {
            const data = line.slice(5).replace(/^ /, "");
            if (data) {
              try {
                const ev = JSON.parse(data);
                events.push(ev);
                if (onEvent) { try { onEvent(ev); } catch {} }
              } catch {}
            }
          }
        }
      }
    } catch (e) {
      return { events, finish: null, error: { code: "stream_aborted", text: String(e?.reason ?? e) } };
    } finally { clearTimeout(timer); }
    const finish = events.find((e) => e.type === "finish") ?? null;
    const err = events.find((e) => e.type === "error") ?? null;
    return { events, finish, error: err ? { code: err.error?.code ?? "unknown", text: err.error?.text ?? "" } : null };
  }

  async ensureProject() {
    if (this.projectId) {
      const r = await this.fetch(`${AGENT}/v4/agent/projects?pageNum=1&pageSize=50`, { timeoutMs: 20_000 });
      const list = r.json?.result?.list ?? [];
      if (list.some((p) => p.id === this.projectId)) return this.projectId;
      this.projectId = null;
    }
    const id = nanoid();
    const r = await this.fetch(`${AGENT}/v4/agent/projects`, { method: "POST", body: { id, name: "meshy2api-gateway" }, timeoutMs: 20_000 });
    if (r.status !== 200) throw new Error(`create project ${r.status}: ${r.text.slice(0, 200)}`);
    this.projectId = r.json.result.id;
    log(`project ready: ${this.projectId}`);
    return this.projectId;
  }

  buildPrefsBody(p, prefs, permissionPolicy) {
    return {
      nickname: p.nickname ?? "",
      custom_instructions: p.custom_instructions ?? "",
      prefs: {
        printers: prefs.printers ?? [],
        image_model: prefs.image_model ?? "",
        image_to_3d_pipeline: prefs.image_to_3d_pipeline ?? "",
        pair_choice_opt_out: prefs.pair_choice_opt_out ?? false,
        permission_policy: permissionPolicy,
      },
    };
  }

  /** Update preferences. patch: { image_model?, image_to_3d_pipeline?, forceAutoApprove? } */
  async setPreferences(patch = {}) {
    const cur = await this.fetch(`${AGENT}/v4/agent/preferences`, { timeoutMs: 20_000 });
    const p = cur.json?.result ?? {};
    const prefs = { ...(p.prefs ?? {}) };
    if ("image_model" in patch) prefs.image_model = patch.image_model;
    if ("image_to_3d_pipeline" in patch) prefs.image_to_3d_pipeline = patch.image_to_3d_pipeline;
    const pp = prefs.permission_policy ?? {};
    const allowedTools = patch.forceAutoApprove === false
      ? (pp.allowed_tools ?? [])
      : [...new Set([...(pp.allowed_tools ?? []), ...ALLOWED_TOOLS])];
    const permissionPolicy = patch.forceAutoApprove === false
      ? { type: pp.type ?? "manual", allowed_tools: allowedTools }
      : { type: "auto", allowed_tools: allowedTools };
    const r = await this.fetch(`${AGENT}/v4/agent/preferences`, { method: "PUT", body: this.buildPrefsBody(p, prefs, permissionPolicy), timeoutMs: 20_000 });
    if (r.status !== 200) throw new Error(`set preferences ${r.status}: ${r.text.slice(0, 200)}`);
    return r.json.result;
  }

  async ensurePrefs() {
    if (this.prefsReady) return;
    await this.setPreferences({});
    this.prefsReady = true;
    log("preferences: auto-approve configured");
  }

  async setPipeline(imageTo3dPipeline) {
    const r = await this.fetch(`${AGENT}/v4/agent/preferences`, { timeoutMs: 20_000 });
    const p = r.json?.result ?? {};
    const prefs = { ...(p.prefs ?? {}) };
    prefs.image_to_3d_pipeline = imageTo3dPipeline;
    const pp = prefs.permission_policy ?? {};
    const body = this.buildPrefsBody(p, prefs, { type: "auto", allowed_tools: [...new Set([...(pp.allowed_tools ?? []), ...ALLOWED_TOOLS])] });
    const r2 = await this.fetch(`${AGENT}/v4/agent/preferences`, { method: "PUT", body, timeoutMs: 20_000 });
    if (r2.status !== 200) throw new Error(`set pipeline ${r2.status}`);
  }

  /** Upload an image to the agent artifact store (agent turn path). */
  async uploadImageArtifact(imageUrlOrDataUrl, { projectId, threadId } = {}) {
    const { bytes, filename } = await loadImageBytes(imageUrlOrDataUrl);
    const ext = filename.split(".").pop() ?? "png";
    const mime = ext === "jpg" ? "image/jpeg" : ext === "webp" ? "image/webp" : "image/png";
    const fd = new FormData();
    fd.append("file", new Blob([bytes], { type: mime }), filename);
    if (projectId) fd.append("project_id", projectId);
    fd.append("artifact_type", "image");
    if (threadId) fd.append("thread_id", threadId);
    const { payload, extra } = await encodeMultipart(fd, "m2a");
    const r = await this.fetch(`${AGENT}/v4/agent/artifacts/upload`, { method: "POST", headers: extra, body: payload, timeoutMs: 60_000, retries: 2 });
    if (r.status !== 200) throw new Error(`upload artifact ${r.status}: ${r.text.slice(0, 200)}`);
    const j = r.json;
    return { artifactId: j.result.artifact_id, url: j.result.url, filename: j.result.filename };
  }

  /**
   * Upload an image and return the UUID used by `imageIds` (3D REST path).
   * NOTE: /v1/files/images returns the UUID; the old agent path
   * (/v4/agent/artifacts/upload) returns `img_upload_*` which imageIds cannot
   * reference → "Image not found".
   */
  async uploadInputImage(imageUrlOrDataUrl, { filename = "input.png" } = {}) {
    const { bytes, filename: name } = await loadImageBytes(imageUrlOrDataUrl, filename);
    const ext4 = (name.split(".").pop() ?? "png").toLowerCase();
    const mime = ext4 === "jpg" || ext4 === "jpeg" ? "image/jpeg" : ext4 === "webp" ? "image/webp" : "image/png";
    const fd = new FormData();
    fd.append("file", new Blob([bytes], { type: mime }), name);
    const { payload, extra } = await encodeMultipart(fd, "m2a");
    const r = await this.fetch(`${MESHYD}/v1/files/images`, { method: "POST", headers: extra, body: payload, timeoutMs: 60_000 });
    if (r.status !== 200) throw new Error(`upload input image ${r.status}: ${String(r.text).slice(0, 200)}`);
    const j = r.json ?? (() => { try { return JSON.parse(r.text); } catch { return null; } })();
    const id = j?.result?.id ?? j?.result?.imageId ?? j?.result?.artifact_id;
    if (!id) throw new Error(`upload input image: no id in response (${String(r.text).slice(0, 160)})`);
    const url = j?.result?.url ?? null;
    const ext = (String(url ?? "").match(/\.([a-z0-9]{2,5})(?:[?#]|$)/i)?.[1]
      ?? String(id).match(/\.([a-z0-9]{2,5})$/i)?.[1]
      ?? ext4 ?? "png").toLowerCase();
    return { id, url, name, ext };
  }

  /** Step 1: create a draft task. `imageId` is the UUID from uploadInputImage. */
  async createDraft(imageId, {
    aiModel, prompt = "", modelType = "standard", multiView = false,
    license = "cc-by-4.0", ext = "png",
    targetPolycount = 10000, shouldTexture = true, enablePBR = true,
  } = {}) {
    const body = {
      phase: "draft",
      args: {
        draft: {
          aiModel, license, shouldTransferImageStyle: true, ultraMode: false, modelType,
          autoSplit: false,
          autoTexture: false,
          multiView: !!multiView, poseMode: "",
          imageIds: [`${String(imageId).replace(/\.[a-z0-9]{2,5}$/i, "")}.${ext}`],
          prompt,
        },
        generate: { draftIds: null, targetPolycount: Number(targetPolycount) || 10000, isUseTexture: !!shouldTexture, enablePBR: !!enablePBR },
      },
    };
    const r = await this.fetch(`${MESHYD}/v2/tasks`, { method: "POST", body, timeoutMs: 30_000 });
    if (r.status !== 200 || r.json?.result == null) {
      const err = new Error(r.json?.result ?? String(r.text).slice(0, 200));
      err.code = r.json?.code ?? `http_${r.status}`;
      throw err;
    }
    const res = r.json.result;
    return typeof res === "string" ? { id: res } : res;
  }

  /** Step 2: kick off generation from a draft (备用 — server auto-chains, so usually unnecessary). */
  async createGenerate(draftTaskId, { targetPolycount = 10000, shouldTexture = true, enablePBR = true, isNSFW = false } = {}) {
    const body = {
      phase: "generate", parent: draftTaskId, status: "PENDING", isNSFW,
      args: { generate: { draftIds: [draftTaskId], isUseTexture: !!shouldTexture, targetPolycount: Number(targetPolycount) || 10000, enablePBR: !!enablePBR } },
    };
    const r = await this.fetch(`${MESHYD}/v2/tasks`, { method: "POST", body, timeoutMs: 30_000 });
    if (r.status !== 200 || r.json?.result == null) {
      const err = new Error(r.json?.result ?? String(r.text).slice(0, 200));
      err.code = r.json?.code ?? `http_${r.status}`;
      throw err;
    }
    const res = r.json.result;
    if (Array.isArray(res)) return { id: res[0], ids: res };
    if (typeof res === "string") return { id: res, ids: [res] };
    return { id: res?.id, ids: [res?.id].filter(Boolean) };
  }

  async getTask(taskId) {
    const r = await this.fetch(`${MESHYD}/v2/tasks/${taskId}`, { timeoutMs: 20_000 });
    return r.status === 200 ? (r.json?.result ?? null) : null;
  }

  async waitTask(taskId, { timeoutMs = 600_000, intervalMs = 4000 } = {}) {
    const deadline = Date.now() + timeoutMs;
    let last = null;
    while (Date.now() < deadline) {
      const t = await this.getTask(taskId).catch(() => null);
      if (t) {
        last = t;
        const running = new Set(["PENDING", "RUNNING", "IN_PROGRESS", "PROCESSING", "QUEUED"]);
        if (!running.has(String(t.status ?? "").toUpperCase())) return t;
      }
      await sleep(intervalMs);
    }
    return last;
  }

  async refreshCredits() {
    const r = await this.fetch(`${MESHYD}/v1/me/credits`, { timeoutMs: 8_000, retries: 1 });
    if (r.status !== 200) return null;
    this.freeCredits = totalCredits(r.json?.result);
    this.creditsBreakdown = r.json?.result ?? this.creditsBreakdown;
    try {
      const t = await this.fetch(`${MESHYD}/v1/me/tier`, { timeoutMs: 8_000, retries: 1 });
      if (t.status === 200 && t.json?.result) {
        this.tierInfo = t.json.result;
        this.refillAt = t.json.result.refillAt ?? null;
        this.monthlyCredits = t.json.result.freeMonthlyCredits ?? null;
      }
    } catch {}
    return this.freeCredits;
  }

  async getRigQuota() {
    const r = await this.fetch(`${MESHYD}/v2/tasks/rig-quota`, { timeoutMs: 15_000 });
    if (r.status !== 200) return null;
    this.rigQuota = r.json?.result ?? null;
    return this.rigQuota;
  }

  async listModelTasks() {
    const r = await this.fetch(`${MESHYD}/v2/tasks?pageNum=1&pageSize=50&sortBy=-created_at`, { timeoutMs: 20_000 });
    const res = r.json?.result;
    const tasks = Array.isArray(res) ? res : (res?.tasks ?? res?.list ?? []);
    return tasks.filter((t) => t.status === "SUCCEEDED" &&
      ["generate", "texture", "image-to-3d-texture", "stylize", "upload", "auto-uv"].includes(t.phase));
  }

  /** Rig a model task. mode: "biped" (needs keypoints) | "smart". */
  async rig(modelTaskId, { mode = "biped", keypoints = null } = {}) {
    const positionJson = mode === "biped" ? JSON.stringify(keypoints ?? DEFAULT_BIPED_KEYPOINTS) : "{}";
    const body = {
      phase: "animate", parent: modelTaskId,
      args: { animate: { showcaseId: "", animationType: mode, fps: 30, offset: 0.5, scale: 1, positionJson, rx: 0, ry: 0, rz: 0 } },
      status: "PENDING", isNSFW: false,
    };
    const r = await this.fetch(`${MESHYD}/v2/tasks`, { method: "POST", body, timeoutMs: 30_000 });
    if (r.status !== 200 || !r.json?.result) {
      const err = new Error(r.json?.result ?? r.text.slice(0, 150));
      err.code = r.json?.code ?? `http_${r.status}`;
      throw err;
    }
    return r.json.result;
  }

  async applyAction(rigTaskId, animationSelection) {
    const r = await this.fetch(`${MESHYD}/v2/tasks/${rigTaskId}/animation-actions`, {
      method: "POST", body: { animationSelection: Number(animationSelection) }, timeoutMs: 90_000,
    });
    if (r.status !== 200 || r.json?.code !== "OK") {
      const err = new Error(r.json?.result ?? r.text.slice(0, 150));
      err.code = r.json?.code ?? `http_${r.status}`;
      throw err;
    }
    return r.json.result;
  }
}

// Tools the bridge needs pre-authorized so turns don't suspend for approval.
const ALLOWED_TOOLS = [
  "text_to_image", "image_to_image", "image_to_3d", "multi_image_to_3d", "text_to_3d",
  "smart_topology", "retexture", "remesh", "texture_multiview", "split_model_into_parts",
  "repair_printability", "analyze_printability", "export_to_slicer", "animate", "rigging",
];

// biped keypoints: normalized x,y,z (0-1), 12 points.
const DEFAULT_BIPED_KEYPOINTS = {
  Chin: { x: 0.5, y: 0.82, z: 0 }, ShoulderA: { x: 0.36, y: 0.77, z: 0 },
  ShoulderB: { x: 0.64, y: 0.77, z: 0 }, ElbowsA: { x: 0.28, y: 0.69, z: 0 },
  ElbowsB: { x: 0.72, y: 0.69, z: 0 }, WristsA: { x: 0.17, y: 0.6, z: 0 },
  WristsB: { x: 0.83, y: 0.6, z: 0 }, Groin: { x: 0.5, y: 0.5, z: 0 },
  KneesA: { x: 0.4, y: 0.28, z: 0 }, KneesB: { x: 0.6, y: 0.28, z: 0 },
  AnkleA: { x: 0.38, y: 0.05, z: 0 }, AnkleB: { x: 0.62, y: 0.05, z: 0 },
};

const gw = new GatewayClient(gatewayCfg);

// Module-level convenience wrappers (used by the HTTP handlers below).
const meshyFetch = (pathname, opts) => gwJson(pathname, opts);
const ensureProject = () => gw.ensureProject();
const ensureToolAutoApprove = () => gw.ensurePrefs();
const setPreferences = (patch) => gw.setPreferences(patch);
const uploadImageArtifact = (url, opts) => gw.uploadImageArtifact(url, opts);
const runTurn = (args) => gw.runTurn(args);

// ---------- account import (credentials.db) ----------

/** Exchange a refresh_token for a fresh session, then probe credits/tier. Throws with a clear message. */
async function verifyCredentials({ refreshToken, accessToken, deviceId, forceRefresh = false }) {
  let session = null;
  const now = Math.floor(Date.now() / 1000);
  // 1) try the existing access_token first (cheap; avoids burning a rotation).
  //    forceRefresh=true skips this → always rotates the refresh_token (keep-alive).
  if (!forceRefresh && accessToken && deviceId) {
    const probe = await probeAccount(accessToken, deviceId).catch(() => null);
    if (probe) session = { access_token: accessToken, expires_at: 0, _credits: probe };
  }
  // 2) fall back to refresh_token
  if (!session) {
    if (!refreshToken) throw new Error("no refresh_token and access_token is unusable");
    const res = await gwFetch(`${SUPABASE_AUTH_URL}/auth/v1/token?grant_type=refresh_token`, {
      method: "POST", noAuth: true, retries: 1,
      headers: { "Content-Type": "application/json", apikey: SUPABASE_ANON_KEY, Authorization: `Bearer ${SUPABASE_ANON_KEY}` },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data?.access_token) {
      const code = data?.error ?? data?.code ?? `http_${res.status}`;
      const msg = data?.error_description ?? data?.msg ?? "";
      throw new Error(`refresh failed (${code}): ${msg}`.slice(0, 180));
    }
    session = {
      access_token: data.access_token,
      refresh_token: data.refresh_token ?? refreshToken,
      expires_at: data.expires_at ?? now + (data.expires_in ?? 900),
      user: data.user ?? null,
    };
  }
  // 3) probe credits + tier with the (possibly existing) token
  const credits = session._credits ?? await probeAccount(session.access_token, deviceId);
  let tier = null;
  try {
    const r = await gwFetch(`${MESHYD}/v1/me/tier`, {
      noAuth: true,
      headers: { Authorization: `Bearer ${session.access_token}`, "X-Device-Id": deviceId, "X-Agent-Protocol-Version": "2", "x-locale": "zh" },
      timeoutMs: 8_000, retries: 0,
    });
    if (r.status === 200) tier = (await r.json())?.result ?? null;
  } catch {}
  return { session, credits, tier };
}

async function probeAccount(token, deviceId) {
  // noAuth: this token may belong to an account that is NOT the active gateway
  // client (import/browser-login happens before activation), so we must not let
  // gwFetch inject the active account's headers.
  const res = await gwFetch(`${MESHYD}/v1/me/credits`, {
    noAuth: true,
    headers: { Authorization: `Bearer ${token}`, "X-Device-Id": deviceId, "X-Agent-Protocol-Version": "2", "x-locale": "zh" },
    timeoutMs: 8_000, retries: 0,
  });
  if (res.status !== 200) return null;
  const j = await res.json().catch(() => null);
  return j?.result ?? null;
}

/** Build a stored record from raw import input; verifies + enriches. */
async function buildAccountRecord({ email, refreshToken, accessToken, deviceId, password, forceRefresh = false }) {
  if (!email) throw new Error("email is required");
  if (!deviceId) throw new Error("deviceId is required (must match the browser's meshy_device_id)");
  if (!refreshToken && !accessToken) throw new Error("refresh_token (or access_token) is required");
  const { session, credits, tier } = await verifyCredentials({ refreshToken, accessToken, deviceId, forceRefresh });
  const resolvedEmail = session.user?.email || email;
  const prev = credStore.get(email) ?? {};
  return {
    ...prev,
    email,
    password: password ?? prev.password ?? null,
    access_token: session.access_token,
    refresh_token: session.refresh_token ?? refreshToken ?? prev.refresh_token ?? null,
    expires_at: session.expires_at || prev.expires_at || 0,
    device_id: deviceId,
    free_credits: totalCredits(credits) ?? prev.free_credits ?? null,
    credits_breakdown: credits ?? prev.credits_breakdown ?? null,
    refill_at: tier?.refillAt ?? prev.refill_at ?? null,
    monthly_credits: tier?.freeMonthlyCredits ?? prev.monthly_credits ?? null,
    user_email: resolvedEmail,
    status: "active",
    created_at: prev.created_at ?? Math.floor(Date.now() / 1000),
    imported_at: Math.floor(Date.now() / 1000),
  };
}

/** Redact secrets for list responses. */
function publicAccount(rec) {
  const dead = accountIsDead(rec.email);
  const hasRt = !!rec.refresh_token;
  const hasDev = !!rec.device_id;
  let health, healthText;
  if (!hasRt || !hasDev) { health = "broken"; healthText = !hasRt ? "缺 refresh_token" : "缺 device_id"; }
  else if (dead) { health = "dead"; healthText = lastAccountError.get(rec.email) || "token 失效(401)，已跳过"; }
  else health = "ok", healthText = "正常";
  return {
    email: rec.email,
    device_id: rec.device_id ?? null,
    free_credits: rec.free_credits ?? null,
    credits_breakdown: rec.credits_breakdown ?? null,
    refill_at: rec.refill_at ?? null,
    monthly_credits: rec.monthly_credits ?? null,
    expires_at: rec.expires_at ?? null,
    status: rec.status ?? "active",
    created_at: rec.created_at ?? null,
    imported_at: rec.imported_at ?? null,
    has_refresh_token: hasRt,
    active: rec.email === gw.email,
    dead,
    health,
    health_text: healthText,
  };
}

// ---------- keep-alive: periodic refresh_token rotation (保号) ----------
// A background timer that refreshes every account's refresh_token on an interval,
// keeping the token chain fresh so the account doesn't silently die. This DOES
// rotate the RT — an account MUST be owned by exactly one holder, otherwise this
// will fight the other holder's rotation (Invalid Refresh Token: Already Used).
const KEEPALIVE = {
  enabled: config.keepAlive?.enabled !== false,
  intervalMinutes: Math.max(1, Number(config.keepAlive?.intervalMinutes) || 30),
  deadRetryMinutes: Math.max(1, Number(config.keepAlive?.deadRetryMinutes) || 60),
};
const keepAliveState = {
  running: false,
  lastRunAt: 0,
  nextRunAt: 0,
  lastResult: null, // { total, ok, dead, errors: [{email,error}], durationMs }
  timer: null,
};

function saveKeepAliveCfg() {
  try {
    credDb.prepare("INSERT INTO meta (key, value) VALUES ('keepalive_cfg', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .run(JSON.stringify({ enabled: KEEPALIVE.enabled, intervalMinutes: KEEPALIVE.intervalMinutes }));
  } catch (e) { credWarn("saveKeepAliveCfg", e); }
}
(function loadKeepAliveCfg() {
  try {
    const row = credDb.prepare("SELECT value FROM meta WHERE key='keepalive_cfg'").get();
    if (row) { const c = JSON.parse(row.value); if (c && typeof c === "object") { if (c.enabled != null) KEEPALIVE.enabled = !!c.enabled; if (c.intervalMinutes) KEEPALIVE.intervalMinutes = Math.max(1, Number(c.intervalMinutes)); } }
  } catch {}
})();

/** Refresh one stored account's token (rotating the RT), persist result. Returns {ok, dead, error, credits}. */
async function refreshOneAccount(rec) {
  try {
    const fresh = await buildAccountRecord({
      email: rec.email, refreshToken: rec.refresh_token, accessToken: rec.access_token,
      deviceId: rec.device_id, password: rec.password, forceRefresh: true,
    });
    credStore.upsert(fresh);
    if (fresh.email === gw.email) gw.loadFrom(fresh);  // keep the live client in sync
    deadAccounts.delete(fresh.email);
    lastAccountError.delete(fresh.email);
    return { ok: true, credits: fresh.free_credits };
  } catch (e) {
    const dead = isDeadTokenError(e);
    if (dead) markAccountDead(rec.email, e?.message);
    else lastAccountError.set(rec.email, String(e?.message).slice(0, 120));
    return { ok: false, dead, error: String(e.message).slice(0, 160) };
  }
}

/** Run one keep-alive pass over every account (serially, to avoid hammering). */
async function runKeepAlive() {
  if (keepAliveState.running) return keepAliveState.lastResult;
  keepAliveState.running = true;
  const t0 = Date.now();
  const results = { total: 0, ok: 0, dead: 0, errors: [] };
  try {
    const accounts = credStore.all().filter((r) => r.email && r.refresh_token && r.device_id);
    results.total = accounts.length;
    for (const rec of accounts) {
      // skip recently-dead accounts until the dead-retry window passes
      if (accountIsDead(rec.email) && (Date.now() - (deadAccounts.get(rec.email) || 0)) < KEEPALIVE.deadRetryMinutes * 60_000) {
        results.dead++; continue;
      }
      const r = await refreshOneAccount(rec);
      if (r.ok) results.ok++;
      else { if (r.dead) results.dead++; results.errors.push({ email: rec.email, error: r.error }); }
      await sleep(300); // gentle pacing between accounts
    }
  } catch (e) {
    log(`keep-alive pass error: ${String(e.message).slice(0, 160)}`);
  } finally {
    keepAliveState.running = false;
    keepAliveState.lastRunAt = Date.now();
    results.durationMs = Date.now() - t0;
    results.finishedAt = new Date().toISOString();
    keepAliveState.lastResult = results;
    log(`keep-alive: ${results.ok}/${results.total} refreshed, ${results.dead} dead, ${results.errors.length} errors (${results.durationMs}ms)`);
  }
  return results;
}

function scheduleKeepAlive() {
  if (keepAliveState.timer) { clearTimeout(keepAliveState.timer); keepAliveState.timer = null; }
  if (!KEEPALIVE.enabled) { keepAliveState.nextRunAt = 0; return; }
  const ms = KEEPALIVE.intervalMinutes * 60_000;
  keepAliveState.nextRunAt = Date.now() + ms;
  keepAliveState.timer = setTimeout(async () => {
    await runKeepAlive().catch(() => {});
    scheduleKeepAlive();
  }, ms);
  // don't hold the event loop open just for the timer
  if (keepAliveState.timer.unref) keepAliveState.timer.unref();
}


function activateAccount(email) {
  const rec = credStore.get(email);
  if (!rec) throw new Error(`unknown account ${email}`);
  gw.loadFrom(rec);
  credStore.setActiveEmail(email);
  gw.persist();
  log(`gateway switched to ${email} device=${(rec.device_id || "").slice(0, 8)}`);
  return rec;
}

// ---------- auto pool replacement (single-account gateway, auto-failover) ----------
// The gateway is single-account, but credentials.db may hold several. When the
// active account's token is dead (401 / rotated by another holder), we skip it and
// hot-swap to the next usable one — same idea as the multi-account pool fallback.

function isDeadTokenError(e) {
  const s = String(e?.message ?? e);
  return /Already Used|Not Found|not valid|is invalid|Invalid Refresh Token|refresh_token_not_found|invalid_grant|invalid token|401|unauthorized/i.test(s);
}

const deadAccounts = new Map(); // email → ts
const lastAccountError = new Map(); // email → last error message (for status display)
const DEAD_ACCOUNT_TTL = 10 * 60 * 1000;
function markAccountDead(email, err) {
  if (email) deadAccounts.set(email, Date.now());
  if (email && err) lastAccountError.set(email, String(err).slice(0, 120));
}
function accountIsDead(email) {
  const ts = deadAccounts.get(email);
  if (!ts) return false;
  if (Date.now() - ts > DEAD_ACCOUNT_TTL) { deadAccounts.delete(email); return false; }
  return true;
}

/** Pick another non-dead account from credentials.db (prefers cached credits), hot-swap into `gw`. */
function autoReplaceAccount(excludeEmail) {
  const candidates = credStore.all()
    .filter((r) => r.email && r.refresh_token && r.device_id && r.email !== excludeEmail && !accountIsDead(r.email))
    .sort((a, b) => (b.free_credits ?? 999) - (a.free_credits ?? 999));
  if (!candidates.length) return null;
  const rec = candidates[0];
  gw.loadFrom(rec);
  credStore.setActiveEmail(rec.email);
  gw.persist();
  return rec.email;
}

// ---------- browser login (CDP: headful browser → read meshy session) ----------
// The gateway controls a *separate*, dedicated browser profile started with
// --remote-debugging-port, opens meshy.ai, and waits for the user to log in.
// It never sees the password — it only reads the cookies Meshy writes to its own
// domain once login succeeds. The debugging port is bound to 127.0.0.1 only.

const BROWSER_CFG = {
  port: Number(config.browser?.port) || 9222,
  path: config.browser?.path || "",             // explicit executable override
  userDataDir: config.browser?.userDataDir || path.join(DATA_DIR, "browser-profile"),
  startUrl: config.browser?.startUrl || "https://www.meshy.ai/",
  headless: config.browser?.headless === true,
};
// persisted overrides (settable from the console, survives restart)
const savedBrowser = (() => {
  const r = credStore; // reuse meta table via a helper below
  try {
    const row = credDb.prepare("SELECT value FROM meta WHERE key='browser_cfg'").get();
    return row ? JSON.parse(row.value) : {};
  } catch { return {}; }
})();
function saveBrowserCfg() {
  try {
    credDb.prepare("INSERT INTO meta (key, value) VALUES ('browser_cfg', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .run(JSON.stringify(BROWSER_CFG));
  } catch (e) { credWarn("saveBrowserCfg", e); }
}
if (savedBrowser && typeof savedBrowser === "object") Object.assign(BROWSER_CFG, savedBrowser);

function findBrowserExecutable() {
  if (BROWSER_CFG.path && fs.existsSync(BROWSER_CFG.path)) return BROWSER_CFG.path;
  const platform = process.platform;
  const candidates = [];
  if (platform === "darwin") {
    candidates.push(
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
      "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
      "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
      "/Applications/Chromium.app/Contents/MacOS/Chromium",
      `${process.env.HOME}/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`,
    );
  } else if (platform === "win32") {
    const pf = process.env["ProgramFiles"] || "C:\\Program Files";
    const pf86 = process.env["ProgramFiles(x86)"] || "C:\\Program Files (x86)";
    const local = process.env["LOCALAPPDATA"] || "";
    candidates.push(
      `${pf}\\Google\\Chrome\\Application\\chrome.exe`,
      `${pf86}\\Google\\Chrome\\Application\\chrome.exe`,
      `${local}\\Google\\Chrome\\Application\\chrome.exe`,
      `${pf}\\Microsoft\\Edge\\Application\\msedge.exe`,
      `${pf86}\\Microsoft\\Edge\\Application\\msedge.exe`,
      `${pf}\\Chromium\\Application\\chrome.exe`,
    );
  } else {
    candidates.push(
      "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
      "/usr/bin/chromium", "/usr/bin/chromium-browser", "/snap/bin/chromium",
    );
    if (process.env.HOME) candidates.push(`${process.env.HOME}/.local/bin/google-chrome`);
  }
  for (const c of candidates) { try { if (c && fs.existsSync(c)) return c; } catch {} }
  return null;
}

async function cdpVersion() {
  try {
    const res = await fetch(`http://127.0.0.1:${BROWSER_CFG.port}/json/version`, { signal: AbortSignal.timeout(1500) });
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}
async function cdpList() {
  try {
    const res = await fetch(`http://127.0.0.1:${BROWSER_CFG.port}/json/list`, { signal: AbortSignal.timeout(2000) });
    if (!res.ok) return [];
    return await res.json();
  } catch { return []; }
}

/** Minimal CDP WebSocket client (global WebSocket, Node 22+). */
function cdpSession(wsUrl) {
  const ws = new WebSocket(wsUrl);
  let id = 0;
  const pending = new Map();
  const ready = new Promise((resolve, reject) => {
    ws.addEventListener("open", () => resolve());
    ws.addEventListener("error", (e) => reject(new Error(`CDP ws error: ${e?.message ?? "connection failed"}`)));
  });
  ws.addEventListener("message", (ev) => {
    let msg; try { msg = JSON.parse(typeof ev.data === "string" ? ev.data : ev.data.toString()); } catch { return; }
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message ?? "CDP error"));
      else resolve(msg.result);
    }
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const mid = ++id;
    pending.set(mid, { resolve, reject });
    try { ws.send(JSON.stringify({ id: mid, method, params })); }
    catch (e) { pending.delete(mid); reject(e); }
    setTimeout(() => { if (pending.has(mid)) { pending.delete(mid); reject(new Error(`CDP ${method} timeout`)); } }, 10_000);
  });
  return { ready, send, close: () => { try { ws.close(); } catch {} } };
}

async function evaluateInTarget(target, expression) {
  const sess = cdpSession(target.webSocketDebuggerUrl);
  await sess.ready;
  try {
    const r = await sess.send("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
    return r?.result?.value ?? null;
  } finally { sess.close(); }
}

/** Read the meshy session (cookies + device id) from a logged-in tab. */
async function captureBrowserSession() {
  const targets = await cdpList();
  const pages = targets.filter((t) => t.type === "page" && /meshy\.ai/.test(t.url || ""));
  const preferred = pages.find((t) => /^https?:\/\/(www\.)?meshy\.ai/.test(t.url)) ?? pages[0];
  if (!preferred) {
    const anyPage = targets.filter((t) => t.type === "page");
    if (!anyPage.length) throw new Error("no page target found in browser");
    throw new Error("no meshy.ai tab found — open meshy.ai and log in first");
  }
  const expr = `(function(){
    var dev = '';
    try { dev = localStorage.getItem('meshy_device_id') || ''; } catch(e) {}
    var c = {};
    var cookieStr = document.cookie || '';
    cookieStr.split('; ').forEach(function(x){ var i=x.indexOf('='); if(i<0) return;
      var n=x.slice(0,i); if(/^sb-auth-auth-token(\\.\\d+)?$/.test(n)) c[n]=x.slice(i+1); });
    return JSON.stringify({ chunks: c, deviceId: dev, href: location.href });
  })()`;
  const raw = await evaluateInTarget(preferred, expr);
  if (!raw) throw new Error("CDP evaluate returned nothing (tab not ready?)");
  const data = JSON.parse(raw);
  if (!data.chunks || !Object.keys(data.chunks).length) {
    throw new Error("not logged in yet — no sb-auth-auth-token cookie on the meshy.ai tab");
  }
  const session = decodeSupabaseChunks(data.chunks);
  if (!session.access_token || !session.refresh_token) throw new Error("captured session is incomplete");
  if (!data.deviceId) throw new Error("captured session has no meshy_device_id (localStorage empty?)");
  // fingerprint lets us tell a *fresh* login from a stale cookie left in the profile
  const fingerprint = crypto.createHash("sha1").update(String(session.refresh_token)).digest("hex").slice(0, 16);
  return { session, deviceId: data.deviceId, email: session.user?.email || "", href: data.href, fingerprint };
}

/** Quick read of the current meshy session fingerprint on any open meshy tab (null if none). */
async function currentMeshyFingerprint() {
  try { return (await captureBrowserSession()).fingerprint; } catch { return null; }
}

/** Clear the browser's cookies so a previous account can't be re-captured. This profile is
 *  only used for login capture, so clearing all cookies is safe. */
async function clearBrowserCookies() {
  const targets = await cdpList();
  const page = targets.find((t) => t.type === "page");
  if (!page) throw new Error("no page target to clear cookies");
  const s = cdpSession(page.webSocketDebuggerUrl);
  await s.ready;
  try {
    await s.send("Network.enable").catch(() => {});
    await s.send("Network.clearBrowserCookies");
  } finally { s.close(); }
}

function decodeSupabaseChunks(chunks) {
  const idx = (k) => { const m = String(k).match(/\.(\d+)$/); return m ? Number(m[1]) : 0; };
  const keys = Object.keys(chunks).sort((a, b) => idx(a) - idx(b));
  let v = keys.map((k) => chunks[k]).join("").replace(/^base64-/, "");
  try { v = decodeURIComponent(v); } catch {}
  const b64 = v.replace(/-/g, "+").replace(/_/g, "/");
  const obj = JSON.parse(Buffer.from(b64, "base64").toString("utf8"));
  return obj.session ?? obj;
}

// login-flow state machine (single in-flight flow)
const loginFlow = {
  status: "idle", // idle | launching | waiting | captured | error
  message: "",
  pid: null,
  startedAt: 0,
  pollTimer: null,
  lastCapture: null,
  error: null,
  launchedByUs: false,
  connected: false,
  baselineFingerprint: null,
};
let loginPollBusy = false;

function loginPublicState() {
  return {
    status: loginFlow.status,
    message: loginFlow.message,
    pid: loginFlow.pid,
    launchedByUs: loginFlow.launchedByUs,
    startedAt: loginFlow.startedAt || null,
    elapsedSeconds: loginFlow.startedAt ? Math.round((Date.now() - loginFlow.startedAt) / 1000) : 0,
    lastCapture: loginFlow.lastCapture,
    error: loginFlow.error,
    browser: { port: BROWSER_CFG.port, path: findBrowserExecutable(), startUrl: BROWSER_CFG.startUrl, connected: !!loginFlow.connected },
  };
}

async function ensureBrowserRunning() {
  const existing = await cdpVersion();
  if (existing) return { connected: true, launched: false, version: existing };
  const exe = findBrowserExecutable();
  if (!exe) throw new Error("no Chrome/Edge/Chromium found — set browser.path in the console 账号 page");
  fs.mkdirSync(BROWSER_CFG.userDataDir, { recursive: true });
  const args = [
    `--remote-debugging-port=${BROWSER_CFG.port}`,
    `--user-data-dir=${BROWSER_CFG.userDataDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--remote-allow-origins=*",
    BROWSER_CFG.startUrl,
  ];
  if (BROWSER_CFG.headless) args.unshift("--headless=new");
  const child = spawn(exe, args, { detached: true, stdio: "ignore" });
  child.unref();
  loginFlow.pid = child.pid;
  loginFlow.launchedByUs = true;
  // wait for the debugging endpoint to come up
  for (let i = 0; i < 40; i++) {
    await sleep(500);
    if (await cdpVersion()) return { connected: true, launched: true, version: await cdpVersion() };
  }
  throw new Error("browser started but the debugging port never became reachable (another Chrome may hold the profile)");
}

function stopLoginPoll() {
  if (loginFlow.pollTimer) { clearInterval(loginFlow.pollTimer); loginFlow.pollTimer = null; }
}
function closeLaunchedBrowser() {
  if (loginFlow.launchedByUs && loginFlow.pid) {
    try { process.kill(-loginFlow.pid, "SIGTERM"); } catch { try { process.kill(loginFlow.pid, "SIGTERM"); } catch {} }
  }
  loginFlow.pid = null;
  loginFlow.launchedByUs = false;
}

async function startBrowserLogin(res, body) {
  if (loginFlow.status === "waiting" || loginFlow.status === "launching") {
    return jsonOk(res, { started: false, ...loginPublicState(), message: "a login flow is already running" });
  }
  if (body && typeof body === "object") {
    if (body.port != null) BROWSER_CFG.port = Number(body.port) || BROWSER_CFG.port;
    if (typeof body.path === "string" && body.path) BROWSER_CFG.path = body.path;
    if (typeof body.userDataDir === "string" && body.userDataDir) BROWSER_CFG.userDataDir = body.userDataDir;
    if (typeof body.startUrl === "string" && body.startUrl) BROWSER_CFG.startUrl = body.startUrl;
    saveBrowserCfg();
  }
  loginFlow.status = "launching";
  loginFlow.message = "launching browser…";
  loginFlow.error = null;
  loginFlow.lastCapture = null;
  loginFlow.startedAt = Date.now();
  loginFlow.connected = false;
  loginFlow.baselineFingerprint = null;
  try {
    const info = await ensureBrowserRunning();
    loginFlow.connected = true;
    // clear cookies left by a previous login so a stale session can't be re-captured
    try { await clearBrowserCookies(); } catch (e) { log(`clear cookies skipped: ${String(e.message).slice(0, 100)}`); }
    loginFlow.baselineFingerprint = await currentMeshyFingerprint();
    loginFlow.status = "waiting";
    loginFlow.message = "browser open — log in a NEW account on meshy.ai; it will be captured automatically";
    stopLoginPoll();
    loginFlow.pollTimer = setInterval(() => { tryCaptureBrowserSession(true); }, 3000);
    jsonOk(res, { started: true, ...loginPublicState() });
  } catch (e) {
    loginFlow.status = "error";
    loginFlow.error = String(e.message);
    loginFlow.message = String(e.message);
    jsonOk(res, { started: false, ...loginPublicState() });
  }
}

async function tryCaptureBrowserSession(auto = false) {
  if (loginPollBusy) return null;
  if (loginFlow.status !== "waiting") return null;
  loginPollBusy = true;
  try {
    const { session, deviceId, email, fingerprint } = await captureBrowserSession();
    // only a session that DIFFERS from the baseline counts as a fresh login
    if (fingerprint === loginFlow.baselineFingerprint) {
      throw new Error("still showing the previous session — clear cookies or log in as a different account");
    }
    stopLoginPoll();
    // verify + enrich, then store (do NOT auto-switch)
    let rec;
    try {
      rec = await buildAccountRecord({
        email: email || `browser-${Date.now()}@meshy`,
        refreshToken: session.refresh_token,
        accessToken: session.access_token,
        deviceId,
      });
    } catch (e) {
      rec = {
        email: email || `browser-${Date.now()}@meshy`,
        access_token: session.access_token,
        refresh_token: session.refresh_token,
        expires_at: session.expires_at ?? 0,
        device_id: deviceId,
        free_credits: null,
        status: "captured",
        created_at: Math.floor(Date.now() / 1000),
        imported_at: Math.floor(Date.now() / 1000),
        verify_error: String(e.message).slice(0, 180),
      };
    }
    credStore.upsert(rec);
    loginFlow.status = "captured";
    loginFlow.message = `captured ${rec.email}${rec.free_credits != null ? ` (${rec.free_credits} credits)` : ""} — stored; switch to it from the list`;
    loginFlow.lastCapture = publicAccount(rec);
    closeLaunchedBrowser();
    log(`browser login captured: ${rec.email} device=${deviceId.slice(0, 8)}`);
    return rec;
  } catch (e) {
    if (!auto) { loginFlow.message = String(e.message); }
    return null;
  } finally { loginPollBusy = false; }
}

async function handleBrowserStart(req, res, body) { return startBrowserLogin(res, body); }
async function handleBrowserCapture(req, res) {
  if (loginFlow.status !== "waiting") return jsonOk(res, { captured: false, ...loginPublicState() });
  const rec = await tryCaptureBrowserSession(false);
  jsonOk(res, { captured: !!rec, account: rec ? publicAccount(rec) : null, ...loginPublicState() });
}
function handleBrowserStatus(res) { jsonOk(res, loginPublicState()); }
function handleBrowserCancel(res) {
  stopLoginPoll();
  closeLaunchedBrowser();
  loginFlow.status = "idle";
  loginFlow.message = "cancelled";
  loginFlow.error = null;
  jsonOk(res, loginPublicState());
}
function handleBrowserConfig(res) {
  jsonOk(res, { port: BROWSER_CFG.port, path: BROWSER_CFG.path, detectedPath: findBrowserExecutable(), userDataDir: BROWSER_CFG.userDataDir, startUrl: BROWSER_CFG.startUrl, headless: BROWSER_CFG.headless });
}


function sniffImageExt(buf, contentType) {
  if (buf.length > 8 && buf[0] === 0x89 && buf[1] === 0x50) return "png";
  if (buf.length > 3 && buf[0] === 0xff && buf[1] === 0xd8) return "jpg";
  if (buf.length > 12 && buf.toString("ascii", 0, 4) === "RIFF" && buf.toString("ascii", 8, 12) === "WEBP") return "webp";
  if (contentType.includes("jpeg")) return "jpg";
  if (contentType.includes("webp")) return "webp";
  return "png";
}

/** Decode a data URL or download a remote image into raw bytes. */
async function loadImageBytes(imageUrlOrDataUrl, defaultName = "input.png") {
  if (imageDataUrlRe.test(String(imageUrlOrDataUrl).slice(0, 64))) {
    const m = String(imageUrlOrDataUrl).match(/^data:([^;,]+)?((?:;[^,]*)?),(.*)$/s);
    const mime = m?.[1] ?? "image/png";
    const bytes = Buffer.from(m[3], "base64");
    const filename = mime.includes("jpeg") ? "input.jpg" : mime.includes("webp") ? "input.webp" : defaultName;
    return { bytes, filename };
  }
  const res = await gwFetch(imageUrlOrDataUrl, { noAuth: true, redirect: "follow", retries: 2 });
  if (!res.ok) throw new Error(`download input image ${res.status}`);
  const ct = (res.headers.get("content-type") ?? "image/png").split(";")[0].trim();
  const bytes = Buffer.from(await res.arrayBuffer());
  return { bytes, filename: `input.${sniffImageExt(bytes, ct)}` };
}

/**
 * Build a multipart body. With a proxy configured we must hand-build the
 * boundary (global fetch's FormData can't be replayed across the tunnel);
 * direct connections can pass the native FormData straight through.
 */
async function encodeMultipart(fd, prefix = "meshy2api") {
  if (!PROXY_URL) return { payload: fd, extra: {} };
  const boundary = `----${prefix}${crypto.randomBytes(12).toString("hex")}`;
  const parts = [];
  for (const [k, v] of fd.entries()) {
    if (v instanceof Blob) {
      parts.push(Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="${k}"; filename="${v.name || "file.png"}"\r\n` +
        `Content-Type: ${v.type || "application/octet-stream"}\r\n\r\n`));
      parts.push(Buffer.from(await v.arrayBuffer()));
      parts.push(Buffer.from("\r\n"));
    } else {
      parts.push(Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${k}"\r\n\r\n${v}\r\n`));
    }
  }
  parts.push(Buffer.from(`--${boundary}--\r\n`));
  const payload = Buffer.concat(parts);
  return { payload, extra: { "Content-Type": `multipart/form-data; boundary=${boundary}`, "Content-Length": String(payload.length) } };
}

// ---------- turn lock (free tier allows only 1 pending task per account) ----------

let turnChain = Promise.resolve();
const turnChainSummary = { depth: 0 };
function withTurnLock(fn) {
  turnChainSummary.depth++;
  const run = turnChain.then(fn, fn).finally(() => { turnChainSummary.depth = Math.max(0, turnChainSummary.depth - 1); });
  turnChain = run.catch(() => {});
  return run;
}

// 3D REST tasks are long-lived pending tasks; free accounts accept only ONE at a
// time (TooManyPendingTasks). Serialize them through a FIFO queue + depth counter.
let task3dChain = Promise.resolve();
const task3dQueue = { depth: 0 };
function with3dSlot(fn) {
  task3dQueue.depth++;
  const run = task3dChain.then(fn, fn).finally(() => { task3dQueue.depth = Math.max(0, task3dQueue.depth - 1); });
  task3dChain = run.catch(() => {});
  return run;
}

// ---------- .meshy → plaintext glTF decryption ----------

const DECRYPT_3D = config.decrypt3dGlb !== false;
const GLB_DIR = path.join(DATA_DIR, "glb");
if (DECRYPT_3D) fs.mkdirSync(GLB_DIR, { recursive: true });
const glbCache = new Map(); // meshyTaskId -> filepath
const glbInflight = new Map();

function glbPathFor(id) { return path.join(GLB_DIR, `${id}.glb`); }

async function downloadAndDecrypt(meshyUrl, cacheKey) {
  if (cacheKey && glbCache.has(cacheKey)) {
    const p = glbCache.get(cacheKey);
    if (fs.existsSync(p)) return p;
    glbCache.delete(cacheKey);
  }
  if (cacheKey && glbInflight.has(cacheKey)) return glbInflight.get(cacheKey);
  const task = (async () => {
    const res = await gwFetch(meshyUrl, { noAuth: true, redirect: "follow", retries: 3 });
    if (!res.ok) throw new Error(`fetch .meshy ${res.status}`);
    const enc = Buffer.from(await res.arrayBuffer());
    const glb = isMeshyEncrypted(enc) ? await decryptMeshy(enc) : enc;
    if (glb.subarray(0, 4).toString("latin1") !== "glTF") throw new Error("decrypt produced non-glTF output");
    const out = cacheKey ? glbPathFor(cacheKey) : path.join(GLB_DIR, `tmp_${nanoid(12)}.glb`);
    fs.writeFileSync(out, glb);
    if (cacheKey) glbCache.set(cacheKey, out);
    return out;
  })();
  if (cacheKey) glbInflight.set(cacheKey, task);
  try { return await task; } finally { if (cacheKey) glbInflight.delete(cacheKey); }
}

/** Recover a plaintext glb for a job from its encrypted model URL. Best-effort. */
async function ensurePlaintextGlb(job) {
  if (!DECRYPT_3D || !job?.meshyTaskId) return null;
  const encUrl = job.result?.glb?.url ?? job.result?.glb?.modelUrl ?? null;
  if (!encUrl) return null;
  try {
    const p = await downloadAndDecrypt(encUrl, job.meshyTaskId);
    const size = fs.statSync(p).size;
    return { path: p, size, url: `/v1/3d/generations/${job.id}/model.glb` };
  } catch (e) {
    log(`glb decrypt failed job=${job.id} task=${job.meshyTaskId}: ${String(e).slice(0, 160)}`);
    return null;
  }
}

function serveGlb(res, id) {
  const p = glbCache.get(id) ?? (fs.existsSync(glbPathFor(id)) ? glbPathFor(id) : null);
  if (!p || !fs.existsSync(p)) return apiError(res, 404, "plaintext glb not available");
  const stat = fs.statSync(p);
  res.writeHead(200, {
    "Content-Type": "model/gltf-binary",
    "Content-Length": stat.size,
    "Content-Disposition": `attachment; filename="${id}.glb"`,
  });
  fs.createReadStream(p).pipe(res);
}

// ---------- sqlite storage (3d jobs / image log) ----------

const DB_FILE = path.join(DATA_DIR, "meshy2api.db");
const db = new DatabaseSync(DB_FILE);
db.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS jobs3d (
    id TEXT PRIMARY KEY,
    seq INTEGER,
    data TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS jobs3d_created ON jobs3d(data);
  CREATE TABLE IF NOT EXISTS image_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at INTEGER NOT NULL,
    model TEXT,
    aspect TEXT,
    quality TEXT,
    credits INTEGER,
    ok INTEGER,
    err TEXT,
    urls TEXT
  );
  CREATE INDEX IF NOT EXISTS image_log_at ON image_log(at DESC);
`);
try {
  const cols = db.prepare("PRAGMA table_info(image_log)").all().map((c) => c.name);
  if (!cols.includes("quality")) db.exec("ALTER TABLE image_log ADD COLUMN quality TEXT");
} catch {}

const dbWarn = (label, e) => log(`sqlite ${label} failed: ${e.message}`);

const jobsDb = {
  seq() {
    try { return db.prepare("SELECT MAX(seq) AS m FROM jobs3d").get()?.m ?? 0; }
    catch (e) { dbWarn("jobs.seq", e); return 0; }
  },
  all() {
    try {
      return db.prepare("SELECT data FROM jobs3d ORDER BY seq ASC").all()
        .map((r) => { try { return JSON.parse(r.data); } catch { return null; } }).filter(Boolean);
    } catch (e) { dbWarn("jobs.all", e); return []; }
  },
  upsert(job) {
    try {
      const m = String(job.id).match(/_([0-9a-z]+)$/);
      const seq = (m ? parseInt(m[1], 36) : 0) || this.seq() + 1;
      db.prepare("INSERT INTO jobs3d (id, seq, data) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data")
        .run(job.id, seq, JSON.stringify(job));
    } catch (e) { dbWarn("jobs.upsert", e); }
  },
  get(id) {
    try {
      const r = db.prepare("SELECT data FROM jobs3d WHERE id = ?").get(id);
      return r ? JSON.parse(r.data) : null;
    } catch (e) { dbWarn("jobs.get", e); return null; }
  },
};

const imageLogDb = {
  add(rec) {
    try {
      db.prepare("INSERT INTO image_log (at, model, aspect, quality, credits, ok, err, urls) VALUES (?,?,?,?,?,?,?,?)")
        .run(rec.at ?? Date.now(), rec.model ?? null, rec.aspect ?? null, rec.quality ?? null,
             rec.credits ?? null, rec.ok ? 1 : 0, rec.err ?? null, JSON.stringify(rec.urls ?? []));
    } catch (e) { dbWarn("image_log.add", e); }
  },
  list(limit = 50) {
    try {
      return db.prepare("SELECT * FROM image_log ORDER BY at DESC LIMIT ?").all(limit)
        .map((r) => ({
          id: r.id, at: r.at, model: r.model, aspect: r.aspect, quality: r.quality,
          credits: r.credits, ok: !!r.ok, err: r.err,
          urls: (() => { try { return JSON.parse(r.urls || "[]"); } catch { return []; } })(),
        }));
    } catch (e) { dbWarn("image_log.list", e); return []; }
  },
  clear() {
    try { db.prepare("DELETE FROM image_log").run(); } catch (e) { dbWarn("image_log.clear", e); }
  },
};

// ---------- animation action library (632 biped actions) ----------

let ACTION_LIBRARY = [];
try {
  const libPath = path.join(ROOT_DIR, "recon", "biped_actions.json");
  if (fs.existsSync(libPath)) {
    ACTION_LIBRARY = JSON.parse(fs.readFileSync(libPath, "utf8"));
    log(`action library loaded: ${ACTION_LIBRARY.length} biped actions`);
  }
} catch (e) { log(`action library load failed: ${e.message}`); }

class JobStore {
  constructor() {
    this.map = new Map();
    this.seq = jobsDb.seq();
    for (const job of jobsDb.all()) this.map.set(job.id, job);
    log(`job store loaded: ${this.map.size} jobs from sqlite`);
  }
  nextId() { return `3d_${Date.now().toString(36)}_${(++this.seq).toString(36)}`; }
  create(job) { this.map.set(job.id, job); jobsDb.upsert(job); return job; }
  get(id) { return this.map.get(id); }
  update(id, patch) {
    const job = this.map.get(id);
    if (!job) return null;
    Object.assign(job, patch);
    jobsDb.upsert(job);
    return job;
  }
  list(limit = 50) { return [...this.map.values()].sort((a, b) => b.createdAt - a.createdAt).slice(0, limit); }
}

const jobs3d = new JobStore();
for (const job of jobs3d.map.values()) {
  if (job.status === "running") {
    if (job.meshyTaskId) { job.status = "recovering"; jobs3d.update(job.id, { status: "recovering" }); }
    else { job.status = "failed"; jobs3d.update(job.id, { status: "failed", error: { code: "restarted", message: "bridge restarted before task id was known" } }); }
  }
}

// ---------- event → result extraction ----------

function extractArtifacts(events) {
  const out = [];
  for (const e of events) {
    if (e.type !== "tool-output-available") continue;
    let arts = [];
    try { arts = e.output?.events?.[0]?.event_data?.artifacts ?? []; } catch {}
    for (const a of arts) {
      const md = a.metadata ?? {};
      out.push({
        artifactId: a.artifact_id, format: md.format ?? null, role: md.role ?? null,
        mimeType: a.mime_type ?? null, url: a.url ?? null, aiModel: md.ai_model ?? null,
        meshyTaskId: md.meshy_task_id ?? null, prompt: md.prompt ?? null,
        aspectRatio: md.aspect_ratio ?? null, thumbnailUrl: md.thumbnail_url ?? null,
      });
    }
  }
  return out;
}

function extractProgress(events) {
  let latest = null;
  for (const e of events) {
    if (e.type === "tool-progress" && e.data?.task_id) {
      latest = { meshyTaskId: e.data.task_id, status: e.data.status ?? null, progress: typeof e.data.progress === "number" ? e.data.progress : null };
    }
  }
  return latest;
}

function extractText(events) {
  let text = "";
  for (const e of events) if (e.type === "text-delta") text += e.delta ?? "";
  return text.trim();
}

// ---------- bridge API helpers ----------

function jsonOk(res, obj) { sendJson(res, 200, obj); }
function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}
function apiError(res, status, message, type = "invalid_request_error", code = null) {
  sendJson(res, status, { error: { message, type, code } });
}
function checkAuth(req, res) {
  if (!BRIDGE_API_KEY) return true;
  if ((req.headers.authorization ?? "") === `Bearer ${BRIDGE_API_KEY}`) return true;
  apiError(res, 401, "invalid bridge api key", "authentication_error");
  return false;
}

// GET /v1/models
async function handleModels(res) {
  const r = await meshyFetch(`${AGENT}/v4/agent/pricing`, { timeoutMs: 20_000 });
  if (r.status !== 200) return apiError(res, 502, `pricing ${r.status}`, "api_error");
  const result = r.json?.result ?? {};
  const imageModels = (result.image_models ?? []).map((m) => ({
    id: m.model, object: "model", created: 0, owned_by: "meshy", kind: "image",
    credits: { text_to_image: m.text_to_image, image_to_image: m.image_to_image },
  }));
  const model3d = [];
  for (const t of result.tools ?? []) {
    if (t.tool === "image_to_3d") {
      for (const [id, spec] of Object.entries(MODEL3D_MAP)) {
        if (!spec.aiModel) continue;
        model3d.push({ id, object: "model", created: 0, owned_by: "meshy", kind: "3d", display: spec.display, ai_model: spec.aiModel, credits: t.pricing });
      }
    }
    if (t.tool === "smart_topology") {
      model3d.push({ id: "meshy-t2", object: "model", created: 0, owned_by: "meshy", kind: "3d", display: MODEL3D_MAP["meshy-t2"].display, ai_model: null, credits: t.pricing });
    }
  }
  jsonOk(res, {
    object: "list",
    data: [
      { id: "orchestrator", object: "model", created: 0, owned_by: "meshy", kind: "chat",
        description: "Meshy Agent (Claude Sonnet 5 backend). Chat only — no OpenAI function/tool calls." },
      { id: "claude-sonnet-5", object: "model", created: 0, owned_by: "meshy", kind: "chat",
        description: "Alias of orchestrator (Meshy Agent, Claude Sonnet 5 backend)." },
      ...imageModels, ...model3d,
    ],
  });
}

function chatUsage(finish, text) {
  const completionTokens = text ? Math.max(1, Math.ceil(text.length / 4)) : 0;
  return { prompt_tokens: 0, completion_tokens: completionTokens, total_tokens: completionTokens, meshy_credits: finish?.usage?.credits ?? 0 };
}

// POST /v1/chat/completions
async function handleChatCompletions(req, res, body) {
  const stream = body.stream === true;
  const messages = Array.isArray(body.messages) ? body.messages : [];
  if (!messages.length) return apiError(res, 400, "messages is required");

  const texts = [];
  for (const m of messages) {
    const c = typeof m.content === "string" ? m.content
      : Array.isArray(m.content) ? m.content.map((p) => (typeof p === "string" ? p : p?.text ?? "")).join("") : "";
    if (!c.trim()) continue;
    const role = m.role === "system" ? "System" : m.role === "assistant" ? "Assistant" : "User";
    texts.push(`${role}: ${c}`);
  }
  const prompt = texts.join("\n\n");

  const projectId = await ensureProject();
  await ensureToolAutoApprove();
  const content = [{ type: "text", text: prompt }];
  const id = `chatcmpl_${Date.now().toString(36)}_${nanoid(8)}`;
  const created = Math.floor(Date.now() / 1000);
  const model = body.model ?? "orchestrator";

  if (stream) {
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", Connection: "keep-alive" });
    const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);
    const chunk = (delta, finishReason = null) =>
      send({ id, object: "chat.completion.chunk", created, model, choices: [{ index: 0, delta, finish_reason: finishReason }] });
    let closed = false;
    let textSoFar = "";
    res.on("close", () => { closed = true; });
    chunk({ role: "assistant", content: "" });
    try {
      const { finish, error } = await runTurn({
        projectId, content, timeoutSeconds: D.imageTurnSeconds,
        onEvent: (ev) => {
          if (closed) return;
          if (ev.type === "text-delta" && ev.delta) { textSoFar += ev.delta; chunk({ content: ev.delta }); }
        },
      });
      if (error && !finish) chunk({ content: `[error] ${error.code}: ${error.text}` });
      chunk({}, "stop");
      send({ id, object: "chat.completion.chunk", created, model, choices: [], usage: chatUsage(finish, textSoFar) });
      res.write("data: [DONE]\n\n");
    } catch (e) {
      if (!closed) chunk({ content: `[error] ${String(e)}` });
    } finally { res.end(); }
    return;
  }

  const { events, finish, error } = await runTurn({ projectId, content, timeoutSeconds: D.imageTurnSeconds });
  if (error && !finish) return apiError(res, mapUpstreamStatus(error), `meshy: ${error.code}: ${error.text}`, "api_error", error.code);
  const text = extractText(events);
  jsonOk(res, {
    id, object: "chat.completion", created, model,
    choices: [{ index: 0, message: { role: "assistant", content: text }, finish_reason: "stop" }],
    usage: chatUsage(finish, text),
    meshy: { credits: finish?.usage?.credits ?? null },
  });
}

function validAspect(v) {
  const ok = new Set(["1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"]);
  return typeof v === "string" && ok.has(v) ? v : null;
}

const IMAGE_STYLE_LABELS = {
  "japanese-anime": "Japanese Anime", "western-realism": "Western Realism",
  "chibi-blind-box": "Chibi Blind Box", "vinyl-figure": "Vinyl Figure", "brick-figure": "Brick Figure",
};
function imageOptionText(body) {
  const parts = [];
  const style = IMAGE_STYLE_LABELS[body.style] ?? (typeof body.style === "string" && body.style.trim() && !IMAGE_STYLE_LABELS[body.style] ? body.style.trim() : null);
  if (style) parts.push(`with style "${style}"`);
  if (body.pose === "a") parts.push("in A-pose");
  else if (body.pose === "t") parts.push("in T-pose");
  if (body.multi_view) parts.push("as a multi-view (turnaround) sheet");
  return parts.length ? ", " + parts.join(", ") : "";
}

function mapUpstreamStatus(err) {
  if (err.code === "insufficient_credit") return 402;
  if (err.code === "rate_limit") return 429;
  if (err.code === "invalid_input") return 400;
  return 502;
}

// POST /v1/image/generation
async function handleImageGeneration(req, res, body) {
  const model = body.model ?? DEFAULTS.imageModel;
  if (!IMAGE_MODELS.has(model)) return apiError(res, 400, `unknown image model ${model}; supported: ${[...IMAGE_MODELS].join(", ")}`);
  const prompt = String(body.prompt ?? "").trim();
  if (!prompt) return apiError(res, 400, "prompt is required");
  const aspectRatio = validAspect(body.aspect_ratio ?? body.size ?? DEFAULTS.aspectRatio);
  const n = Math.max(1, Math.min(8, Number(body.n ?? 1) || 1));
  const qualityAllowed = /nano-banana/.test(model) ? ["standard", "hd"] : ["standard", "hd", "ultra"];
  const quality = qualityAllowed.includes(body.quality) ? body.quality : "standard";
  const wantImg2Img = Array.isArray(body.image) && body.image.length > 0;

  const projectId = await ensureProject();
  await ensureToolAutoApprove();

  const content = [];
  if (wantImg2Img) {
    for (const img of body.image.slice(0, 5)) {
      const up = await uploadImageArtifact(img, { projectId });
      content.push({ type: "artifact", artifact_id: up.artifactId });
    }
  }
  const toolText = wantImg2Img ? "image_to_image" : "text_to_image";
  content.push({
    type: "text",
    text: `Use the ${toolText} tool with model ${model}${aspectRatio ? ` and aspect_ratio ${aspectRatio}` : ""} to generate ${n > 1 ? n + " images" : "one image"}${imageOptionText(body)}: ${prompt}`,
  });

  log(`image turn start model=${model} img2img=${wantImg2Img} stream=${!!body.stream}`);
  if (body.stream) return streamImage(res, { projectId, content, model, prompt, n });

  const { events, finish, error } = await runTurn({ projectId, content, timeoutSeconds: D.imageTurnSeconds });
  if (error && !finish) return apiError(res, mapUpstreamStatus(error), `meshy: ${error.code}: ${error.text}`, "api_error", error.code);
  const arts = extractArtifacts(events).filter((a) => a.format === "png" || a.mimeType === "image/png" || (a.mimeType ?? "").startsWith("image/"));
  const primary = arts.filter((a) => a.role === "primary");
  const chosen = (primary.length ? primary : arts).slice(0, n);
  if (!chosen.length) {
    log(`image no artifacts; eventTypes=${events.map((e) => e.type).join(",")}; text=${extractText(events).slice(0, 300)}`);
    return apiError(res, 502, `meshy produced no image artifacts. text: ${extractText(events).slice(0, 200)}`, "api_error");
  }
  const usage = finish?.usage ?? null;
  log(`image done credits=${usage?.credits ?? "?"} quality=${quality}`);
  jsonOk(res, {
    created: Math.floor(Date.now() / 1000),
    data: chosen.map((a) => ({ url: a.url, revised_prompt: a.prompt ?? prompt })),
    usage, quality,
    meshy: { artifacts: chosen.map((a) => ({ artifact_id: a.artifactId, meshy_task_id: a.meshyTaskId, aspect_ratio: a.aspectRatio, ai_model: a.aiModel })), quality },
  });
}

// POST /v1/image/generation {stream:true} → OpenAI images SSE chunks
async function streamImage(res, { projectId, content, model, prompt, n }) {
  const id = `img_${Date.now().toString(36)}_${nanoid(8)}`;
  const created = Math.floor(Date.now() / 1000);
  res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache, no-transform", Connection: "keep-alive" });
  const send = (obj) => res.write(`data: ${JSON.stringify(obj)}\n\n`);
  let closed = false;
  res.on("close", () => { closed = true; });
  try {
    const { events, finish, error } = await runTurn({
      projectId, content, timeoutSeconds: D.imageTurnSeconds,
      onEvent: (ev) => {
        if (closed) return;
        if (ev.type === "tool-progress" && ev.data?.progress != null) send({ id, object: "image.generation.chunk", created, progress: ev.data.progress, status: ev.data.status });
        else if (ev.type === "text-delta" && ev.delta) send({ id, object: "image.generation.chunk", created, text: ev.delta });
      },
    });
    if (error && !finish) { send({ id, object: "image.generation.error", created, error: { code: error.code, message: error.text } }); return res.end(); }
    const arts = extractArtifacts(events).filter((a) => (a.mimeType ?? "").startsWith("image/"));
    const primary = arts.filter((a) => a.role === "primary");
    const chosen = (primary.length ? primary : arts).slice(0, n);
    send({
      id, object: "image.generation", created,
      data: chosen.map((a) => ({ url: a.url, revised_prompt: a.prompt ?? prompt })),
      usage: finish?.usage ?? null,
      meshy: { artifacts: chosen.map((a) => ({ artifact_id: a.artifactId, meshy_task_id: a.meshyTaskId, ai_model: a.aiModel })) },
    });
    res.write("data: [DONE]\n\n");
  } catch (e) {
    if (!closed) send({ id, object: "image.generation.error", created, error: { code: "bridge_error", message: String(e) } });
  } finally { res.end(); }
}

// POST /v1/3d/generations
async function handle3dGeneration(req, res, body) {
  const rawModel = body.model ?? DEFAULTS.model3d;
  const spec = resolve3dModel(rawModel);
  if (!spec) return apiError(res, 400, `unknown 3d model ${rawModel}; supported: ${Object.keys(MODEL3D_MAP).join(", ")} (aliases: ${Object.keys(MODEL3D_ALIASES).join(", ")})`);
  const model = MODEL3D_ALIASES[rawModel] ?? rawModel;
  const prompt = String(body.prompt ?? "").trim();
  const imageUrl = body.image_url ?? body.image ?? null;
  if (!imageUrl) return apiError(res, 400, "image_url is required (meshy image_to_3d needs an input image)");
  const targetPolycount = Number(body.target_polycount ?? 0) || null;
  const shouldTexture = body.should_texture ?? true;
  const quality = body.quality === "ultra" ? "ultra" : "standard";
  const wait = body.wait === true || body.wait === "true";

  const projectId = await ensureProject();

  // ---- REST direct-draft only produces UNTEXTURED models (phase=generate). To get
  //      textures we must use the agent turn path (image_to_3d tool + should_texture),
  //      which yields phase=image-to-3d-texture. So REST is used only when texture is off. ----
  if (spec.aiModel && !shouldTexture) {
    const id = jobs3d.nextId();
    const job = { id, status: "running", model, prompt, projectId, createdAt: Date.now(), account: gw.email || "gateway", protocol: "rest" };
    jobs3d.create(job);
    log(`3d(rest) start model=${model} aiModel=${spec.aiModel} job=${id}`);

    // serialize pending REST tasks: free accounts allow only ONE pending task
    // at a time, so queue the whole job (upload → draft → wait) FIFO.
    (async () => with3dSlot(async () => {
      try {
        const up = await gw.uploadInputImage(imageUrl);
        job.inputImageId = up.id;
        jobs3d.update(id, { inputImageId: up.id });

        const draft = await gw.createDraft(up.id, {
          ext: up.ext, targetPolycount, shouldTexture, aiModel: spec.aiModel, prompt,
          modelType: body.model_type === "lowpoly" ? "lowpoly" : "standard", multiView: body.multi_view === true,
        });
        jobs3d.update(id, { draftTaskId: draft.id, meshyTaskId: draft.id, status: "generating" });
        log(`3d(rest) draft created ${draft.id}`);
        // the server auto-chains draft→generate into the same task; track that id
        const done = await gw.waitTask(draft.id, { timeoutMs: D.model3dTurnSeconds * 1000 });

        job.draftTaskId = draft.id;
        job.meshyTaskId = draft.id;
        const st = String(done?.status ?? "UNKNOWN").toUpperCase();
        let result = done?.result ?? null;
        if (st === "SUCCEEDED") {
          const gen = result?.generate ?? null;
          const modelUrl = gen?.modelUrl ?? result?.modelUrl ?? null;
          const enriched = { ...(result ?? {}) };
          if (done?.name) enriched.name = done.name;
          if (modelUrl) enriched.glb = { url: modelUrl };
          if (result?.previewUrl) enriched.previewUrl = result.previewUrl;
          result = enriched;
          log(`3d(rest) job ${id} model url: ${modelUrl ? modelUrl.slice(0, 90) : "(none)"}`);
        }
        jobs3d.update(id, {
          status: st === "SUCCEEDED" ? "succeeded" : (st === "FAILED" || st === "CANCELED" ? "failed" : "running"),
          meshy_status: st, result,
          error: st === "SUCCEEDED" ? null : `upstream ${st}`,
        });
        if (st === "SUCCEEDED") {
          const plain = await ensurePlaintextGlb(jobs3d.get(id)).catch(() => null);
          if (plain) { const cur = jobs3d.get(id); jobs3d.update(id, { result: { ...cur.result, glb_plain: plain } }); }
        }
        await gw.refreshCredits().catch(() => {});
        log(`3d(rest) job ${id} -> ${st}`);
      } catch (e) {
        jobs3d.update(id, { status: "failed", error: String(e?.message ?? e).slice(0, 300) });
        log(`3d(rest) job ${id} failed: ${String(e?.message ?? e).slice(0, 200)}`);
      }
    }))();

    if (wait) {
      while (["running", "generating", "recovering"].includes(jobs3d.get(id)?.status)) await sleep(3000);
      return sendJob(res, jobs3d.get(id), { protocol: "rest", aiModel: spec.aiModel, account: job.account });
    }
    return jsonOk(res, { id, status: "running", model, protocol: "rest", aiModel: spec.aiModel, account: job.account });
  }

  // ---- agent turn protocol (T2, or textured meshy-6/7 — REST can't texture) ----
  await ensureToolAutoApprove();
  await setPreferences({ image_to_3d_pipeline: spec.pipeline });
  const up = await uploadImageArtifact(imageUrl, { projectId });
  const polyText = targetPolycount ? ` Target around ${targetPolycount} polygons.` : "";
  const texText = shouldTexture ? "" : " Do not apply texture (should_texture false).";
  const toolName = spec.pipeline === "smart_topology" ? "smart_topology" : "image_to_3d";
  const content = [
    { type: "artifact", artifact_id: up.artifactId },
    { type: "text", text: `Use the ${toolName} tool on this image to generate a 3D model, ${quality} quality.${polyText}${texText} ${prompt}`.trim() },
  ];

  const id = jobs3d.nextId();
  const job = { id, status: "running", model, prompt, inputArtifactId: up.artifactId, projectId, createdAt: Date.now(), account: gw.email || "gateway" };
  jobs3d.create(job);
  log(`3d turn start model=${model} job=${id}`);

  const p = (async () => {
    try {
      const { events, finish, error } = await runTurn({
        projectId, content, timeoutSeconds: D.model3dTurnSeconds,
        onEvent: (ev) => {
          if (ev.type === "tool-progress" && ev.data?.task_id && job.meshyTaskId !== ev.data.task_id) {
            job.meshyTaskId = ev.data.task_id;
            jobs3d.update(id, { meshyTaskId: job.meshyTaskId });
          }
        },
      });
      job.progress = extractProgress(events);
      if (job.progress?.meshyTaskId) job.meshyTaskId = job.progress.meshyTaskId;
      job.credits = finish?.usage ?? null;
      const arts = extractArtifacts(events);
      const glbs = arts.filter((a) => a.format === "glb");
      const previews = arts.filter((a) => a.format === "png");
      if (!glbs.length) log(`3d job ${id} no glb; eventTypes=${events.map((e) => e.type).join(",")}; err=${JSON.stringify(error ?? {})}`);
      if (error && !glbs.length) {
        jobs3d.update(id, { status: "failed", error: { code: error.code, message: error.text }, credits: job.credits });
        return;
      }
      jobs3d.update(id, {
        status: "succeeded", meshyTaskId: job.meshyTaskId ?? null, credits: job.credits,
        result: { glb: glbs[0] ?? null, thumbnails: previews.map((a) => ({ url: a.url, artifact_id: a.artifactId })), all_artifacts: arts },
      });
      const plain = await ensurePlaintextGlb(jobs3d.get(id));
      if (plain) {
        const cur = jobs3d.get(id);
        jobs3d.update(id, { result: { ...cur.result, glb_plain: plain } });
        log(`3d job ${id} plaintext glb ready (${plain.size} bytes)`);
      }
    } catch (e) {
      jobs3d.update(id, { status: "failed", error: { code: "bridge_error", message: String(e) } });
    }
  })();

  if (wait) { await p; return sendJob(res, jobs3d.get(id)); }
  p.catch(() => {});
  return jsonOk(res, { id, status: "running", model, poll: `/v1/3d/generations/${id}` });
}

function sendJob(res, job, extra = {}) {
  jsonOk(res, {
    id: job.id, status: job.status, model: job.model, created: Math.floor(job.createdAt / 1000),
    meshy_task_id: job.meshyTaskId ?? null, progress: job.progress?.progress ?? null,
    credits: job.credits ?? null, triangleCount: job.triangleCount ?? null,
    result: job.result ?? null, error: job.error ?? null, ...extra,
  });
}

async function handle3dGet(req, res, id) {
  const job = jobs3d.get(id);
  if (!job) return apiError(res, 404, "job not found");
  const extra = {};
  if (job.meshyTaskId) {
    const r = await gw.fetch(`${MESHYD}/v2/tasks/${job.meshyTaskId}`, { timeoutMs: 20_000 }).catch(() => null);
    if (r?.status === 200) {
      const t = r.json?.result ?? {};
      extra.task = {
        status: t.status, phase: t.phase, progress: t.progress, triangleCount: t.triangleCount,
        previewUrl: t.result?.previewUrl ?? null,
        modelUrlEncrypted: t.result?.texture?.modelUrl ?? t.result?.generate?.modelUrl ?? null,
        showWatermark: t.showWatermark,
      };
      const needsRecovery = job.status === "recovering" || job.status === "running" ||
        (job.status === "succeeded" && !(job.result?.glb?.url || (job.result?.thumbnails ?? []).length));
      if (needsRecovery) {
        const backfill = {};
        if (t.cost != null && job.credits?.credits == null) backfill.credits = { credits: t.cost, by_tool: { recovered: t.cost } };
        if (t.triangleCount != null) backfill.triangleCount = t.triangleCount;
        if (t.status === "SUCCEEDED") {
          const modelUrl = t.result?.texture?.modelUrl ?? t.result?.generate?.modelUrl ?? null;
          const previewUrl = t.result?.previewUrl ?? null;
          jobs3d.update(id, {
            status: "succeeded", ...backfill,
            result: {
              glb: modelUrl ? { artifactId: null, format: "glb", url: modelUrl, role: "primary", meshyTaskId: job.meshyTaskId } : null,
              thumbnails: previewUrl ? [{ url: previewUrl, artifact_id: null }] : [],
              all_artifacts: [], recovered: true,
            },
          });
          const plain = await ensurePlaintextGlb(jobs3d.get(id));
          if (plain) { const cur = jobs3d.get(id); jobs3d.update(id, { result: { ...cur.result, glb_plain: plain } }); }
        } else if (t.status === "FAILED" || t.status === "CANCELED") {
          jobs3d.update(id, { status: "failed", ...backfill, error: { code: t.status.toLowerCase(), message: t.task_error?.message ?? t.status } });
        } else {
          extra.progress = t.progress;
        }
      } else if (job.credits?.credits == null && t.cost != null) {
        jobs3d.update(id, { credits: { credits: t.cost, by_tool: { recovered: t.cost } }, triangleCount: t.triangleCount ?? null });
      }
      if (jobs3d.get(id)?.triangleCount == null && t.triangleCount != null) jobs3d.update(id, { triangleCount: t.triangleCount });
    }
  }
  sendJob(res, jobs3d.get(id), extra);
}

// GET /v1/image/quota
async function handleImageQuota(res) {
  try {
    const r = await meshyFetch(`${MESHYD}/v2/tasks/image-quota`, { timeoutMs: 15_000 });
    if (r.status !== 200) return apiError(res, r.status, `quota ${r.status}`, "api_error");
    return jsonOk(res, r.json);
  } catch (e) {
    return apiError(res, 502, `quota fetch failed: ${e.message}`, "api_error");
  }
}

// ---------- animate endpoints (rigging + apply-motion) ----------

const animateJobs = new Map();
let animateSeq = 0;

function animTierOf(sel) {
  const meta = ACTION_LIBRARY.find((a) => a.selection === sel);
  return meta?.free ? "free" : "pro";
}

// POST /v1/3d/animate { model_task_id?, image_url?, mode?, actions:[...], wait? }
async function handleAnimate(req, res, body) {
  const mode = body.mode === "smart" ? "smart" : "biped";
  const wanted = Array.isArray(body.actions) ? body.actions.map(Number).filter((n) => !Number.isNaN(n)) : [];
  const allowPaid = body.allow_paid === true || body.allow_paid === "true";
  if (!allowPaid) {
    const paidRequested = wanted.filter((s) => animTierOf(s) === "pro");
    if (paidRequested.length) {
      const names = paidRequested.map((s) => ACTION_LIBRARY.find((a) => a.selection === s)?.name ?? s);
      return apiError(res, 403, `These actions require a paid tier (Pro), not allowed for free accounts: ${names.join(", ")}. Pass allow_paid:true to force.`);
    }
  }

  let modelTaskId = body.model_task_id ?? null;
  if (!modelTaskId) {
    const models = await gw.listModelTasks().catch(() => []);
    if (models.length) modelTaskId = models[0].id;
  }
  if (!modelTaskId) return apiError(res, 400, "model_task_id required (or account must have a finished 3D model)");

  const quota = await gw.getRigQuota().catch(() => null);
  if (quota && quota.remaining != null && quota.remaining <= 0) {
    return apiError(res, 429, `rig quota exhausted (daily_limit=${quota.daily_limit})`);
  }

  const id = `anim_${Date.now().toString(36)}_${(++animateSeq).toString(36)}`;
  const job = { id, status: "rigging", account: gw.email || "gateway", modelTaskId, mode, createdAt: Date.now(), actions: [] };
  animateJobs.set(id, job);
  log(`animate: rig ${modelTaskId} mode=${mode} job=${id}`);

  const p = with3dSlot(async () => {
    try {
      const rigTaskId = await gw.rig(modelTaskId, { mode, keypoints: body.keypoints ?? null });
      job.rigTaskId = rigTaskId;
      job.status = "rig_queued";
      const deadline = Date.now() + (D.model3dTurnSeconds * 1000);
      let task = null;
      while (Date.now() < deadline) {
        await sleep(6000);
        task = await gw.getTask(rigTaskId);
        if (!task) continue;
        job.progress = task.progress;
        if (task.status === "SUCCEEDED" || task.status === "FAILED") break;
      }
      if (!task || task.status !== "SUCCEEDED") {
        job.status = "failed";
        job.error = { code: "rig_failed", message: task?.errorDetail ?? task?.errMsg ?? task?.status ?? "timeout" };
        return;
      }
      job.status = "applying";
      for (const sel of wanted) {
        try {
          await gw.applyAction(rigTaskId, sel);
          const meta = ACTION_LIBRARY.find((a) => a.selection === sel);
          job.actions.push({ selection: sel, name: meta?.name ?? null, tier: meta?.free ? "free" : "pro", ok: true });
        } catch (e) {
          const meta2 = ACTION_LIBRARY.find((a) => a.selection === sel);
          job.actions.push({ selection: sel, name: meta2?.name ?? null, tier: meta2?.free ? "free" : "pro", ok: false, error: { code: e.code, message: e.message } });
        }
      }
      const finalTask = await gw.getTask(rigTaskId);
      const anim = finalTask?.result?.animate ?? {};
      job.status = "succeeded";
      job.result = {
        rig_task_id: rigTaskId, animation_type: anim.animationType ?? mode,
        actions: (anim.actions ?? []).map((a) => ({ actionType: a.actionType, type: a.type, animationGlbUrl: a.animationGlbUrl, armatureGlbUrl: a.armatureGlbUrl ?? null })),
        quadJsonUrl: anim.quadJsonUrl ?? null,
      };
      await gw.getRigQuota().catch(() => {});
      log(`animate ${id} done: ${job.result.actions.length} actions`);
    } catch (e) {
      job.status = "failed";
      job.error = { code: e.code ?? "bridge_error", message: String(e.message ?? e).slice(0, 200) };
    }
  });

  if (body.wait === true || body.wait === "true") { await p; return sendAnimJob(res, job); }
  p.catch(() => {});
  return jsonOk(res, { id, status: "rigging", poll: `/v1/3d/animate/${id}`, account: job.account });
}

function sendAnimJob(res, job) {
  jsonOk(res, {
    id: job.id, status: job.status, account: job.account, mode: job.mode,
    model_task_id: job.modelTaskId, rig_task_id: job.rigTaskId ?? null,
    progress: job.progress ?? null, created: Math.floor(job.createdAt / 1000),
    actions: job.actions ?? [], result: job.result ?? null, error: job.error ?? null,
  });
}

async function handleAnimateGet(res, id) {
  const job = animateJobs.get(id);
  if (!job) return apiError(res, 404, "animate job not found");
  sendAnimJob(res, job);
}

// ---------- one-click pipeline: image → 3D → animate ----------

// POST /v1/pipeline { prompt?, image_url?, image_model?, model3d?, actions?, ... }
async function handlePipeline(req, res, body) {
  const imageModel = body.image_model ?? DEFAULTS.imageModel;
  const model3d = body.model3d ?? "meshy-6";
  const actions = Array.isArray(body.actions) && body.actions.length ? body.actions.map(Number) : [-2];
  const mode = body.mode === "smart" ? "smart" : "biped";
  const allowPaid = body.allow_paid === true;
  const targetPolycount = Number(body.target_polycount ?? 0) || null;
  const prompt = String(body.prompt ?? "").trim();
  let inputImageUrl = body.image_url ?? null;

  const stages = { image: null, model3d: null, animate: null };
  const t0 = Date.now();

  if (!allowPaid) {
    const paid = actions.filter((s) => !(ACTION_LIBRARY.find((a) => a.selection === s)?.free));
    if (paid.length) return apiError(res, 403, `actions require paid tier: ${paid.join(", ")}. Pass allow_paid:true`);
  }

  const quota = await gw.getRigQuota().catch(() => null);
  if (quota && quota.remaining != null && quota.remaining <= 0) {
    return apiError(res, 429, `no rig quota left today (daily_limit=${quota.daily_limit})`);
  }

  const run = async () => {
    if (!inputImageUrl) {
      if (!prompt) throw new Error("prompt or image_url is required");
      const projectId = await gw.ensureProject();
      await gw.ensurePrefs();
      const content = [{ type: "text", text: `Use the text_to_image tool with model ${imageModel} to generate one image: ${prompt}` }];
      const { events, finish, error } = await gw.runTurn({ projectId, content, timeoutSeconds: D.imageTurnSeconds });
      if (error && !finish) throw new Error(`image: ${error.code}: ${error.text}`);
      const arts = extractArtifacts(events).filter((a) => (a.mimeType ?? "").startsWith("image/"));
      const chosen = (arts.filter((a) => a.role === "primary").length ? arts.filter((a) => a.role === "primary") : arts)[0];
      if (!chosen) throw new Error("image stage produced no artifact");
      inputImageUrl = chosen.url;
      stages.image = { url: chosen.url, artifact_id: chosen.artifactId, credits: finish?.usage?.credits ?? null };
      log("pipeline image done");
    }

    const projectId2 = await gw.ensureProject();
    await gw.ensurePrefs();
    await gw.setPipeline(MODEL3D_MAP[model3d] ?? MODEL3D_MAP["meshy-6"]);
    const up = await gw.uploadImageArtifact(inputImageUrl, { projectId: projectId2 });
    const content3d = [
      { type: "artifact", artifact_id: up.artifactId },
      { type: "text", text: `Use the image_to_3d tool on this image to generate a 3D model of a humanoid character, standard quality.${targetPolycount ? ` Target around ${targetPolycount} polygons.` : ""}` },
    ];
    let meshyTaskId = null;
    const { events: ev3d, finish: fin3d, error: err3d } = await gw.runTurn({
      projectId: projectId2, content: content3d, timeoutSeconds: D.model3dTurnSeconds,
      onEvent: (ev) => { if (ev.type === "tool-progress" && ev.data?.task_id) meshyTaskId = ev.data.task_id; },
    });
    if (err3d && !extractArtifacts(ev3d).some((a) => a.format === "glb")) throw new Error(`3d: ${err3d.code ?? "error"}: ${err3d.text ?? ""}`);
    const modelJobId = jobs3d.nextId();
    const art3d = extractArtifacts(ev3d);
    const glbs = art3d.filter((a) => a.format === "glb");
    const previews = art3d.filter((a) => a.format === "png");
    jobs3d.create({
      id: modelJobId, status: glbs.length ? "succeeded" : "failed", model: model3d, prompt,
      inputArtifactId: up.artifactId, projectId: projectId2, createdAt: Date.now(), account: gw.email || "gateway",
      meshyTaskId: meshyTaskId ?? null, credits: fin3d?.usage ?? null,
      result: glbs.length ? { glb: glbs[0], thumbnails: previews.map((a) => ({ url: a.url, artifact_id: a.artifactId })), all_artifacts: art3d } : null,
    });
    let glbPlain = null;
    if (glbs.length && DECRYPT_3D) {
      glbPlain = await ensurePlaintextGlb(jobs3d.get(modelJobId)).catch(() => null);
      if (glbPlain) { const cur = jobs3d.get(modelJobId); jobs3d.update(modelJobId, { result: { ...cur.result, glb_plain: glbPlain } }); }
    }
    stages.model3d = { job_id: modelJobId, meshy_task_id: meshyTaskId, glb: glbs[0] ?? null, glb_plain: glbPlain, preview: previews[0]?.url ?? null, credits: fin3d?.usage?.credits ?? null };
    log(`pipeline 3d done task=${meshyTaskId} glb_plain=${glbPlain ? glbPlain.size : "none"}`);

    if (!meshyTaskId) throw new Error("3d stage produced no meshy task id (cannot rig)");

    const rigTaskId = await gw.rig(meshyTaskId, { mode, keypoints: body.keypoints ?? null });
    const deadline = Date.now() + D.model3dTurnSeconds * 1000;
    let rigTask = null;
    while (Date.now() < deadline) {
      await sleep(6000);
      rigTask = await gw.getTask(rigTaskId);
      if (rigTask && (rigTask.status === "SUCCEEDED" || rigTask.status === "FAILED")) break;
    }
    if (!rigTask || rigTask.status !== "SUCCEEDED") throw new Error(`rig failed: ${rigTask?.errorDetail ?? rigTask?.status ?? "timeout"}`);
    for (const sel of actions) { try { await gw.applyAction(rigTaskId, sel); } catch (e) { log(`pipeline apply ${sel} failed: ${e.message}`); } }
    const finalTask = await gw.getTask(rigTaskId);
    const anim = finalTask?.result?.animate ?? {};
    stages.animate = {
      rig_task_id: rigTaskId, animation_type: anim.animationType ?? mode,
      actions: (anim.actions ?? []).map((a) => ({ actionType: a.actionType, type: a.type, animationGlbUrl: a.animationGlbUrl })),
    };
    log(`pipeline animate done: ${stages.animate.actions.length} actions`);
    return stages;
  };

  try {
    const result = await run();
    jsonOk(res, { status: "succeeded", account: gw.email || "gateway", elapsed_seconds: Math.round((Date.now() - t0) / 100) / 10, stages: result });
  } catch (e) {
    jsonOk(res, { status: "failed", account: gw.email || "gateway", elapsed_seconds: Math.round((Date.now() - t0) / 100) / 10, stages, error: { code: e.code ?? "pipeline_error", message: String(e.message ?? e).slice(0, 250) } });
  }
}

// GET /v1/3d/animations — built-in action library
function handleActions(res, query) {
  const cat = query.get("category");
  const search = (query.get("search") ?? "").toLowerCase();
  const tierFilter = query.get("tier");
  let data = ACTION_LIBRARY;
  if (cat) data = data.filter((a) => a.category === cat);
  if (search) data = data.filter((a) => a.name.toLowerCase().includes(search));
  if (tierFilter === "free") data = data.filter((a) => a.free);
  else if (tierFilter === "pro") data = data.filter((a) => !a.free);
  const freeCount = data.filter((a) => a.free).length;
  const out = data.map((a) => ({ ...a, tier: a.free ? "free" : "pro", usable_by: a.free ? "free" : "paid" }));
  jsonOk(res, {
    object: "list", total: out.length, free_count: freeCount, pro_count: out.length - freeCount,
    requested_tier: tierFilter || "all",
    categories: [...new Set(ACTION_LIBRARY.map((a) => a.category).filter(Boolean))],
    data: out,
  });
}

// ---------- stats (single account) ----------

const upstreamCache = { credits: { at: 0, val: null }, info: { at: 0, val: null } };
const UPSTREAM_TTL = 60_000;

async function probeUpstream(pathname) {
  let token = gw.accessToken;
  const fresh = token && (gw.expiresAt - Math.floor(Date.now() / 1000)) > 60;
  if (!fresh) token = await gw.getToken().catch(() => null);
  if (!token) return null;
  try {
    const res = await gwFetch(pathname.startsWith("http") ? pathname : `${ORIGIN}${pathname}`, {
      headers: { Authorization: `Bearer ${token}`, "X-Device-Id": gw.deviceId, "X-Agent-Protocol-Version": "2", "x-locale": "zh" },
      timeoutMs: 6_000, retries: 0,
    });
    if (res.status !== 200) return null;
    const j = await res.json().catch(() => null);
    return j?.result ?? null;
  } catch { return null; }
}

async function cachedUpstream(kind, pathname) {
  const slot = upstreamCache[kind];
  if (slot.val && Date.now() - slot.at < UPSTREAM_TTL) return slot.val;
  const val = await probeUpstream(pathname);
  if (val) { slot.val = val; slot.at = Date.now(); return val; }
  return slot.val;
}

let upstreamRefreshInFlight = false;
let upstreamLastAttempt = 0;
function scheduleUpstreamRefresh() {
  if (upstreamRefreshInFlight || Date.now() - upstreamLastAttempt < 10_000) return;
  upstreamRefreshInFlight = true;
  upstreamLastAttempt = Date.now();
  (async () => {
    try {
      const [credits, info] = await Promise.all([
        probeUpstream(`${MESHYD}/v1/me/credits`),
        probeUpstream(`${MESHYD}/v1/me/info`),
      ]);
      if (credits) { upstreamCache.credits.val = credits; upstreamCache.credits.at = Date.now(); }
      if (info) { upstreamCache.info.val = info; upstreamCache.info.at = Date.now(); }
    } catch {} finally { upstreamRefreshInFlight = false; }
  })();
}

async function handleStats(res) {
  const jobs = jobs3d.list(1000);
  const succeeded = jobs.filter((j) => j.status === "succeeded");
  const failed = jobs.filter((j) => j.status === "failed");
  const running = jobs.filter((j) => j.status === "running" || j.status === "recovering" || j.status === "generating");
  const creditsSpent = jobs.reduce((s, j) => s + (j.credits?.credits ?? 0), 0);
  scheduleUpstreamRefresh();
  const upstream = upstreamCache.credits.val ?? gw.creditsBreakdown ?? null;
  const info = upstreamCache.info.val ?? null;
  const total = totalCredits(upstream);
  const nowMs = Date.now();
  jsonOk(res, {
    bridge: {
      port: PORT, version: 2, startedAt: BRIDGE_STARTED_AT,
      tokenExpiresAt: gw.expiresAt || null, decrypt3d: DECRYPT_3D, dataDir: DATA_DIR,
      turnQueueWaiting: turnChainSummary.depth,
      pending3d: task3dQueue.depth,
      keepAlive: { enabled: KEEPALIVE.enabled, intervalMinutes: KEEPALIVE.intervalMinutes, nextRunAt: KEEPALIVE.enabled ? keepAliveState.nextRunAt || null : null, lastRunAt: keepAliveState.lastRunAt || null },
    },
    jobs: { total: jobs.length, succeeded: succeeded.length, failed: failed.length, running: running.length, creditsSpent },
    upstream, upstreamTotal: total,
    currentAccount: info?.email ?? gw.email ?? null,
    account: {
      email: info?.email ?? gw.email ?? null,
      freeCredits: gw.freeCredits,
      totalCredits: total,
      refillAt: gw.refillAt,
      refillInSeconds: gw.refillAt ? Math.max(0, Math.floor((gw.refillAt - nowMs) / 1000)) : null,
      monthlyCredits: gw.monthlyCredits ?? 100,
      monthlyBucket: upstream?.freeCreditBalance ?? 0,
      permanentBucket: (upstream?.creditBalance ?? 0) + (upstream?.shareCreditEarned ?? 0),
      expiresAt: gw.expiresAt || null,
      pool: {
        total: credStore.all().length,
        dead: [...deadAccounts.keys()],
        activeIsDead: accountIsDead(gw.email),
      },
    },
  });
}

// ---------- account management handlers ----------

function handleAccountsList(res) {
  const accounts = credStore.all().map((r) => ({ ...publicAccount(r), dead: accountIsDead(r.email) }));
  jsonOk(res, { object: "list", total: accounts.length, active: gw.email || null, data: accounts });
}

async function handleAccountImport(req, res, body) {
  const email = String(body.email ?? "").trim();
  const refreshToken = String(body.refresh_token ?? body.refreshToken ?? "").trim();
  const accessToken = String(body.access_token ?? body.accessToken ?? "").trim();
  const deviceId = String(body.device_id ?? body.deviceId ?? "").trim();
  const password = typeof body.password === "string" ? body.password : undefined;
  if (!email) return apiError(res, 400, "email is required");
  if (!deviceId) return apiError(res, 400, "device_id is required and must match the browser's meshy_device_id (otherwise DeviceKicked / rule1_same_platform)");
  let rec;
  try {
    rec = await buildAccountRecord({ email, refreshToken, accessToken, deviceId, password });
  } catch (e) {
    return apiError(res, 400, `import failed: ${String(e.message).slice(0, 200)}`, "invalid_input");
  }
  credStore.upsert(rec);
  const makeActive = body.activate !== false; // default: activate on import
  if (makeActive || !credStore.activeEmail()) activateAccount(email);
  log(`account imported: ${email} credits=${rec.free_credits} active=${makeActive}`);
  jsonOk(res, { imported: true, activated: makeActive, account: publicAccount(rec) });
}

/**
 * Read an uploaded credentials DB. Accepts either:
 *   - multipart/form-data with a `file` field (binary) plus optional `emails`
 *     (repeated field or a JSON array string), or
 *   - application/json { data: base64, emails?: [...] } (legacy, kept for compat)
 * Lands the DB on disk in DATA_DIR; returns { path, filename, emails }.
 * Caller must delete `path`.
 */
async function readUploadedDb(req) {
  const ct = String(req.headers["content-type"] ?? "");
  let filename = "uploaded.db";
  let buf = null;
  let emails = [];
  if (ct.startsWith("multipart/form-data")) {
    const form = await new Request("http://localhost", { method: "POST", headers: req.headers, body: req, duplex: "half" }).formData();
    const file = form.get("file") ?? [...form.values()].find((v) => v instanceof Blob);
    if (!file) { const e = new Error("multipart body has no file field"); e.statusCode = 400; throw e; }
    filename = (file.name && String(file.name)) || filename;
    buf = Buffer.from(await file.arrayBuffer());
    for (const v of form.getAll("emails")) {
      const s = String(v);
      try { const arr = JSON.parse(s); if (Array.isArray(arr)) { emails = arr.map(String); continue; } } catch {}
      if (s) emails.push(s);
    }
  } else {
    const body = await readJsonBody(req);
    const b64 = String(body.data ?? "");
    if (!b64) { const e = new Error("data (base64 of the .db file) is required"); e.statusCode = 400; throw e; }
    filename = String(body.filename ?? filename);
    try { buf = Buffer.from(b64, "base64"); } catch { const e = new Error("data is not valid base64"); e.statusCode = 400; throw e; }
    if (Array.isArray(body.emails)) emails = body.emails.map(String);
  }
  if (!buf?.length) { const e = new Error("uploaded file is empty"); e.statusCode = 400; throw e; }
  const tmp = path.join(DATA_DIR, `_upload_${nanoid(12)}.db`);
  fs.writeFileSync(tmp, buf);
  return { path: tmp, filename, emails };
}

/** Open a temp DB file read-only, run fn(source, helpers), always clean up. */
async function withTempDb(filePath, fn) {
  let source;
  try {
    source = new DatabaseSync(filePath, { readOnly: true });
    const has = (name) => {
      try { return source.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(name)?.name === name; }
      catch { return false; }
    };
    if (!has("accounts")) { const e = new Error("uploaded DB has no `accounts` table (expected the meshy2api credentials DB)"); e.statusCode = 400; throw e; }
    return await fn(source);
  } finally {
    try { source?.close(); } catch {}
    try { fs.unlinkSync(filePath); } catch {}
  }
}

/** Parse an uploaded credentials DB and return the accounts WITHOUT importing. */
async function handleAccountUploadParse(req, res) {
  let up;
  try { up = await readUploadedDb(req); }
  catch (e) { return apiError(res, e.statusCode ?? 400, String(e.message).slice(0, 160)); }
  try {
    const list = await withTempDb(up.path, (source) => {
      const rows = source.prepare("SELECT data FROM accounts").all();
      const out = [];
      for (const r of rows) {
        try {
          const a = JSON.parse(r.data);
          if (!a?.email) continue;
          out.push({
            email: a.email,
            device_id: a.device_id ?? null,
            free_credits: a.free_credits ?? null,
            has_refresh_token: !!a.refresh_token,
            has_access_token: !!a.access_token,
            created_at: a.created_at ?? null,
            already_imported: !!credStore.get(a.email),
          });
        } catch {}
      }
      return out;
    });
    jsonOk(res, { object: "list", filename: up.filename, total: list.length, data: list });
  } catch (e) {
    return apiError(res, e.statusCode ?? 400, `cannot read DB: ${String(e.message).slice(0, 160)}`);
  }
}

/** Import selected accounts from an uploaded credentials DB. */
async function handleAccountImportFromDb(req, res) {
  let up;
  try { up = await readUploadedDb(req); }
  catch (e) { return apiError(res, e.statusCode ?? 400, String(e.message).slice(0, 160)); }
  return _importFromDb(res, up, up.emails);
}

async function _importFromDb(res, up, emails) {
  if (!emails.length) {
    try { fs.unlinkSync(up.path); } catch {}
    return apiError(res, 400, "emails[] is required");
  }
  const results = [];
  try {
    await withTempDb(up.path, async (source) => {
      const rows = source.prepare("SELECT data FROM accounts").all();
      const byEmail = new Map();
      for (const r of rows) { try { const a = JSON.parse(r.data); if (a?.email) byEmail.set(a.email, a); } catch {} }
      for (const email of emails) {
        const src = byEmail.get(email);
        if (!src) { results.push({ email, ok: false, error: "not found in uploaded DB" }); continue; }
        try {
          const rec = await buildAccountRecord({
            email: src.email, refreshToken: src.refresh_token, accessToken: src.access_token,
            deviceId: src.device_id, password: src.password,
          });
          credStore.upsert(rec);
          results.push({ email, ok: true, free_credits: rec.free_credits, device_id: rec.device_id });
        } catch (e) {
          results.push({ email, ok: false, error: String(e.message).slice(0, 180) });
        }
      }
    });
    jsonOk(res, { imported: results.filter((r) => r.ok).length, results });
  } catch (e) {
    return apiError(res, e.statusCode ?? 400, `import failed: ${String(e.message).slice(0, 160)}`);
  }
}

async function handleAccountSwitch(req, res, body) {
  const email = String(body.email ?? "").trim();
  if (!email) return apiError(res, 400, "email is required");
  try {
    const rec = activateAccount(email);
    jsonOk(res, { switched: true, active: email, account: publicAccount(rec), tokenExpiresAt: gw.expiresAt });
  } catch (e) {
    return apiError(res, 404, String(e.message));
  }
}

async function handleAccountRefresh(req, res, body) {
  const email = String(body.email ?? "").trim();
  const rec = email ? credStore.get(email) : (gw.email ? credStore.get(gw.email) : null);
  if (!rec) return apiError(res, 404, "account not found");
  try {
    const fresh = await buildAccountRecord({
      email: rec.email, refreshToken: rec.refresh_token, accessToken: rec.access_token,
      deviceId: rec.device_id, password: rec.password,
    });
    credStore.upsert(fresh);
    if (fresh.email === gw.email) gw.loadFrom(fresh);
    jsonOk(res, { refreshed: true, account: publicAccount(fresh) });
  } catch (e) {
    return apiError(res, 400, `refresh failed: ${String(e.message).slice(0, 180)}`);
  }
}

async function handleAccountDelete(req, res, query) {
  const email = query.get("email");
  if (!email) return apiError(res, 400, "email query param is required");
  if (email === credStore.activeEmail()) return apiError(res, 409, "cannot delete the active account; switch first");
  const existed = !!credStore.get(email);
  credStore.remove(email);
  jsonOk(res, { removed: existed ? 1 : 0 });
}

function handleActiveAccount(res) {
  jsonOk(res, {
    active: gw.email || null,
    device_id: gw.deviceId ? `${gw.deviceId.slice(0, 8)}…` : null,
    free_credits: gw.freeCredits,
    credits_breakdown: gw.creditsBreakdown,
    refill_at: gw.refillAt,
    tokenExpiresAt: gw.expiresAt || null,
    in_store: !!credStore.get(gw.email),
  });
}

// ---------- keep-alive handlers ----------
function keepAlivePublicState() {
  return {
    enabled: KEEPALIVE.enabled,
    intervalMinutes: KEEPALIVE.intervalMinutes,
    deadRetryMinutes: KEEPALIVE.deadRetryMinutes,
    running: keepAliveState.running,
    lastRunAt: keepAliveState.lastRunAt || null,
    nextRunAt: KEEPALIVE.enabled ? keepAliveState.nextRunAt || null : null,
    nextRunInSeconds: KEEPALIVE.enabled && keepAliveState.nextRunAt ? Math.max(0, Math.round((keepAliveState.nextRunAt - Date.now()) / 1000)) : null,
    lastResult: keepAliveState.lastResult,
    accounts: credStore.all().length,
  };
}
function handleKeepAliveStatus(res) { jsonOk(res, keepAlivePublicState()); }
async function handleKeepAliveRun(req, res) {
  const r = await runKeepAlive();
  jsonOk(res, { ...keepAlivePublicState(), thisRun: r });
}
function handleKeepAliveConfig(req, res, body) {
  if (typeof body.enabled === "boolean") KEEPALIVE.enabled = body.enabled;
  if (body.intervalMinutes != null) KEEPALIVE.intervalMinutes = Math.max(1, Number(body.intervalMinutes) || KEEPALIVE.intervalMinutes);
  if (body.deadRetryMinutes != null) KEEPALIVE.deadRetryMinutes = Math.max(1, Number(body.deadRetryMinutes) || KEEPALIVE.deadRetryMinutes);
  saveKeepAliveCfg();
  scheduleKeepAlive();
  jsonOk(res, keepAlivePublicState());
}

// ---------- http server ----------

async function readBody(req, limit = 25 * 1024 * 1024) {
  const chunks = [];
  let size = 0;
  for await (const c of req) {
    size += c.length;
    if (size > limit) throw new Error("body too large");
    chunks.push(c);
  }
  return Buffer.concat(chunks).toString("utf8");
}

/** Read + JSON-parse a request body; throws a 400-able error on malformed JSON. */
async function readJsonBody(req, res) {
  const raw = await readBody(req);
  if (!raw) return {};
  try { return JSON.parse(raw); }
  catch { const e = new Error("invalid JSON body"); e.statusCode = 400; throw e; }
}

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://localhost");
  const p = u.pathname.replace(/\/+$/, "") || "/";
  try {
    if (p === "/health") return jsonOk(res, { ok: true, email: gw.email || null, expiresAt: gw.expiresAt || null });

    if (p === "/console" || p === "/console/") {
      const html = fs.readFileSync(path.join(ROOT_DIR, "console.html"));
      res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Content-Length": html.length });
      return res.end(html);
    }

    if (!checkAuth(req, res)) return;

    // account-management + browser-login routes work even when no account is configured
    if (p.startsWith("/v1/gateway/")) {
      if (req.method === "GET" && p === "/v1/gateway/accounts") return handleAccountsList(res);
      if (req.method === "POST" && p === "/v1/gateway/accounts") {
        const body = await readJsonBody(req, res);
        return await handleAccountImport(req, res, body);
      }
      if (req.method === "DELETE" && p === "/v1/gateway/accounts") return await handleAccountDelete(req, res, u.searchParams);
      if (req.method === "POST" && p === "/v1/gateway/accounts/upload") {
        return await handleAccountUploadParse(req, res);
      }
      if (req.method === "POST" && p === "/v1/gateway/accounts/import-db") {
        return await handleAccountImportFromDb(req, res);
      }
      if (req.method === "POST" && p === "/v1/gateway/accounts/switch") {
        const body = await readJsonBody(req, res);
        return await handleAccountSwitch(req, res, body);
      }
      if (req.method === "POST" && p === "/v1/gateway/accounts/refresh") {
        const body = await readJsonBody(req, res);
        return await handleAccountRefresh(req, res, body);
      }
      if (req.method === "GET" && p === "/v1/gateway/active") return handleActiveAccount(res);
      if (req.method === "GET" && p === "/v1/gateway/keepalive") return handleKeepAliveStatus(res);
      if (req.method === "POST" && p === "/v1/gateway/keepalive/run") return await handleKeepAliveRun(req, res);
      if (req.method === "POST" && p === "/v1/gateway/keepalive/config") {
        const body = await readJsonBody(req, res);
        return handleKeepAliveConfig(req, res, body);
      }
      if (req.method === "GET" && p === "/v1/gateway/login/browser/config") return handleBrowserConfig(res);
      if (req.method === "GET" && p === "/v1/gateway/login/browser/status") return handleBrowserStatus(res);
      if (req.method === "POST" && p === "/v1/gateway/login/browser/start") {
        const body = await readJsonBody(req, res);
        return await handleBrowserStart(req, res, body);
      }
      if (req.method === "POST" && p === "/v1/gateway/login/browser/capture") return await handleBrowserCapture(req, res);
      if (req.method === "POST" && p === "/v1/gateway/login/browser/cancel") return handleBrowserCancel(res);
    }

    // everything below needs a working upstream account
    if (!gw.deviceId || (!gw.accessToken && !gw.refreshToken)) {
      return apiError(res, 503, "no account configured — import one and activate it (console 账号 page)", "api_error", "no_account");
    }

    if (req.method === "GET" && p === "/v1/models") return await handleModels(res);

    if (req.method === "GET" && p === "/v1/stats") return await handleStats(res);
    if (req.method === "GET" && p === "/v1/image/quota") return await handleImageQuota(res);

    if (p === "/v1/image-log") {
      if (req.method === "GET") return jsonOk(res, { results: imageLogDb.list(100) });
      if (req.method === "POST") {
        const body = await readJsonBody(req, res);
        imageLogDb.add(body);
        return jsonOk(res, { added: true });
      }
      if (req.method === "DELETE") { imageLogDb.clear(); return jsonOk(res, { cleared: true }); }
    }

    if (req.method === "POST" && p === "/v1/chat/completions") {
      const body = await readJsonBody(req, res);
      return await handleChatCompletions(req, res, body);
    }
    if (req.method === "POST" && p === "/v1/image/generation") {
      const body = await readJsonBody(req, res);
      return await handleImageGeneration(req, res, body);
    }
    if (req.method === "POST" && p === "/v1/3d/generations") {
      const body = await readJsonBody(req, res);
      return await handle3dGeneration(req, res, body);
    }
    if (req.method === "POST" && p === "/v1/3d/animate") {
      const body = await readJsonBody(req, res);
      return await handleAnimate(req, res, body);
    }
    if (req.method === "POST" && p === "/v1/pipeline") {
      const body = await readJsonBody(req, res);
      return await handlePipeline(req, res, body);
    }
    if (req.method === "GET" && p === "/v1/3d/animations") return handleActions(res, u.searchParams);
    const mAnim = p.match(/^\/v1\/3d\/animate\/([^/]+)$/);
    if (req.method === "GET" && mAnim) return await handleAnimateGet(res, mAnim[1]);

    if (req.method === "GET" && p === "/v1/3d/generations") {
      const limit = Number(u.searchParams.get("limit") ?? 50);
      const data = [];
      for (const j of jobs3d.list(limit)) {
        const row = {
          id: j.id, status: j.status, model: j.model, created: Math.floor(j.createdAt / 1000),
          meshy_task_id: j.meshyTaskId ?? null, credits: j.credits ?? null, error: j.error ?? null,
          triangleCount: j.triangleCount ?? null,
        };
        if (j.result?.glb_plain) row.glb_plain = { url: j.result.glb_plain.url, size: j.result.glb_plain.size };
        if (j.result?.glb?.url) row.glbUrl = j.result.glb.url;
        if (j.result?.thumbnails?.[0]?.url) row.previewUrl = j.result.thumbnails[0].url;
        data.push(row);
      }
      if (DECRYPT_3D) {
        for (const row of data) {
          if (row.status === "succeeded" && !row.glb_plain && row.glbUrl) {
            const job = jobs3d.get(row.id);
            const plain = await ensurePlaintextGlb(job).catch(() => null);
            if (plain) {
              const cur = jobs3d.get(row.id);
              jobs3d.update(row.id, { result: { ...cur.result, glb_plain: plain } });
              row.glb_plain = { url: plain.url, size: plain.size };
            }
          }
        }
      }
      return jsonOk(res, { object: "list", data });
    }

    const mGlb = p.match(/^\/v1\/3d\/generations\/([^/]+)\/model\.glb$/);
    if (req.method === "GET" && mGlb) {
      const job = jobs3d.get(mGlb[1]);
      if (job) {
        const plain = await ensurePlaintextGlb(job);
        if (plain) {
          const cur = jobs3d.get(mGlb[1]);
          if (!cur.result?.glb_plain) jobs3d.update(mGlb[1], { result: { ...cur.result, glb_plain: plain } });
        }
        return serveGlb(res, job.meshyTaskId);
      }
      return serveGlb(res, mGlb[1]);
    }

    const m3 = p.match(/^\/v1\/3d\/generations\/([^/]+)$/);
    if (req.method === "GET" && m3) return await handle3dGet(req, res, m3[1]);

    return apiError(res, 404, `no route ${req.method} ${p}`);
  } catch (e) {
    log("error:", e?.stack ?? e);
    if (!res.headersSent) {
      const status = e?.statusCode === 400 ? 400 : (e?.message === "body too large" ? 413 : 500);
      apiError(res, status, String(e?.message ?? e), status === 500 ? "internal_error" : "invalid_request_error");
    }
  }
});

server.listen(PORT, HOST, () => {
  const storeN = credStore.all().length;
  log(`meshy2api-gateway listening on http://${HOST}:${PORT}`);
  log(`credentials.db=${CRED_DB_FILE} accounts=${storeN}`);
  log(`gateway account=${gw.email || "(email unknown)"} device=${gw.deviceId.slice(0, 8)} proxy=${PROXY_URL ? `${PROXY_URL.host}:${PROXY_URL.port}` : "direct"}`);
  if (HOST !== "127.0.0.1" && HOST !== "localhost" && !BRIDGE_API_KEY) {
    log("⚠️  WARNING: bound to a non-local interface with NO apiKey set — anyone who can reach");
    log("   this port can read/import/switch accounts and spawn a debug browser. Set config.apiKey!");
  }
  if (gw.email) credStore.setActiveEmail(gw.email);
  gw.refreshCredits()
    .then((c) => log(`credits=${c ?? "?"} (monthly=${gw.creditsBreakdown?.freeCreditBalance ?? "?"} permanent=${(gw.creditsBreakdown?.creditBalance ?? 0) + (gw.creditsBreakdown?.shareCreditEarned ?? 0)})`))
    .catch((e) => log(`startup credit probe failed: ${String(e.message).slice(0, 120)}`));
  // keep-alive: periodically rotate every account's refresh_token (保号)
  if (KEEPALIVE.enabled && storeN > 0) {
    scheduleKeepAlive();
    log(`keep-alive: enabled, every ${KEEPALIVE.intervalMinutes} min over ${storeN} accounts`);
  } else {
    log(`keep-alive: ${KEEPALIVE.enabled ? "no accounts to keep alive" : "disabled"}`);
  }
});
