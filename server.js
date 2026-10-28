'use strict';

const path = require('path');
const fs = require('fs/promises');
const crypto = require('crypto');
const express = require('express');
const { Pool } = require('pg');

// ---------- Configuration ----------
const PORT = Number(process.env.PORT) || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  console.error('DATABASE_URL is not set. Copy .env.example to .env and add your Neon connection string.');
  process.exit(1);
}
const AI_BASE_URL = process.env.AI_BASE_URL || 'https://apis.keithsite.lol/';
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS) || 90_000;
const TEST_TIMEOUT_MS = 30_000;
const MAX_URL_LENGTH = 7000;          // the Keith API only accepts GET query strings
const MAX_PROVIDER_TRIES = 3;
const MAX_FILE_ATTEMPTS = 3;          // one generation plus up to two repairs
const BREAKER_THRESHOLD = 3;
const BREAKER_COOLDOWN_MS = 120_000;
const DEFAULT_CHAT = 'ai_gpt';
const FALLBACK_ORDER = ['ai_claudeai', 'ai_gemini', 'ai_deepseek', 'keithai', 'ai_mistral'];
const MAX_VERSIONS = 10;
const MAX_FILE_CHARS = 200_000;
const REQUIRED_DEPS = { express: '^4.21.2', pg: '^8.13.1' };
const STARTER_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New app</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#07080D;color:#EEF0F7;font-family:system-ui,sans-serif}</style>
</head>
<body><p>Describe your app and press Build to generate it.</p></body>
</html>`;

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FILE_PATH_RE = /^[A-Za-z0-9_][A-Za-z0-9._/-]{0,119}$/;
const safePath = (p) => typeof p === 'string' && FILE_PATH_RE.test(p) && !p.includes('..');
const isUuid = (s) => UUID_RE.test(String(s));
const now = () => new Date().toISOString();

// ---------- Database ----------
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: true }, max: 5 });
const q = (text, params) => pool.query(text, params);

// ---------- Provider registry (all 34 endpoints from the Keith APIs page) ----------
const idOf = (p) => p.slice(1).replace(/\//g, '_');
const CHAT_PATHS = [
  '/keithai', '/ai/gpt', '/ai/claudeai', '/ai/mistral', '/ai/copilot', '/ai/bard',
  '/ai/perplexity', '/ai/o3', '/ai/chatgpt4', '/ai/venice', '/ai/deepseekr', '/ai/gpt4',
  '/ai/qwenai', '/ai/metai', '/ai/deepseek', '/ai/blackbox', '/ai/ilama', '/ai/gemini', '/ai/grok',
];
const PROVIDERS = [
  ...CHAT_PATHS.map((path) => ({ path, kind: 'chat', example: { q: 'hi' } })),
  { path: '/ai/wormgpt', kind: 'disabled', example: { q: 'hi' }, note: 'Locked: unfiltered persona, never used' },
  { path: '/search/ai', kind: 'research', example: { q: 'who is God' } },
  { path: '/ai/searchai', kind: 'research', example: { query: 'Kenya News today' } },
  { path: '/whatdoesthiscodedo', kind: 'research', example: { code: "console.log('hi keith');" } },
  { path: '/ai/vision', kind: 'media', example: { image: 'https://i.imgur.com/t5fTpbk.jpeg', q: "What's in this image" } },
  { path: '/ai/text2img', kind: 'media', example: { q: 'am a good boy' } },
  { path: '/ai/lyricsgen', kind: 'media', example: { topic: 'Heartbreak', genre: 'blues', mood: 'sad', structure: 'verse_chorus_bridge', language: 'en' } },
  { path: '/ai/shazam', kind: 'media', example: { url: 'https://files.catbox.moe/dirk7v.m4a' } },
  { path: '/ai/txt2music', kind: 'media', example: { q: 'am a good boy' } },
  { path: '/ai/vocalremover', kind: 'media', example: { url: 'https://files.catbox.moe/xcy8kh.mp3' } },
  { path: '/ai/transcribe', kind: 'media', example: { q: 'https://keith-files.vercel.app/rtx5wq.mp3' } },
  { path: '/ai/tts', kind: 'media', example: { q: 'hi' } },
  { path: '/ai/txt2video2', kind: 'media', example: { q: 'am a good boy' } },
  { path: '/ai/dreamanalyzer', kind: 'media', example: { q: 'I dreamt about frog chasing me' } },
  { path: '/ai/speechwriter', kind: 'media', example: { topic: 'how to pass examination', length: 'short', type: 'dedication', tone: 'serious' } },
].map((p) => ({ ...p, id: idOf(p.path) }));
const PROVIDER_BY_ID = new Map(PROVIDERS.map((p) => [p.id, p]));

// Enabled state is stored in Neon; a missing row means enabled.
const providerSettings = new Map();

// ---------- Live events ----------
const streams = new Map(); // projectId -> Set of SSE responses

function publish(projectId, type, data) {
  const set = streams.get(projectId);
  if (!set) return;
  const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of set) res.write(msg);
}

const toEvent = (r) => ({ id: Number(r.id), at: r.created_at, agent: r.agent, level: r.level, msg: r.message });

async function emit(projectId, buildId, agent, level, message) {
  const msg = String(message).slice(0, 500);
  const r = await q(
    'INSERT INTO project_events (project_id, build_id, agent, level, message) VALUES ($1,$2,$3,$4,$5) RETURNING id, created_at',
    [projectId, buildId, agent, level, msg]
  );
  publish(projectId, 'log', toEvent({ id: r.rows[0].id, created_at: r.rows[0].created_at, agent, level, message: msg }));
}

async function setStatus(projectId, status) {
  await q('UPDATE projects SET status=$2, updated_at=now() WHERE id=$1', [projectId, status]);
  publish(projectId, 'status', { status });
}

async function snapshot(projectId) {
  const st = await q('SELECT status FROM projects WHERE id=$1', [projectId]);
  const latest = await q(
    'SELECT build_id FROM project_events WHERE project_id=$1 AND build_id IS NOT NULL ORDER BY id DESC LIMIT 1',
    [projectId]
  );
  let log = [];
  if (latest.rows[0]) {
    const ev = await q(
      'SELECT id, created_at, agent, level, message FROM project_events WHERE project_id=$1 AND build_id=$2 ORDER BY id DESC LIMIT 200',
      [projectId, latest.rows[0].build_id]
    );
    log = ev.rows.reverse().map(toEvent);
  }
  return { status: st.rows[0]?.status || 'draft', log };
}

// ---------- Provider health and calls ----------
const health = new Map(); // in memory; resets on restart

function healthOf(id) {
  if (!health.has(id)) {
    health.set(id, {
      calls: 0, ok: 0, failed: 0, lastLatencyMs: null, lastError: null,
      lastOkAt: null, consecutiveFails: 0, cooldownUntil: 0,
    });
  }
  return health.get(id);
}

function record(id, ok, latencyMs, error) {
  const h = healthOf(id);
  h.calls++;
  h.lastLatencyMs = latencyMs;
  if (ok) {
    h.ok++;
    h.lastOkAt = now();
    h.lastError = null;
    h.consecutiveFails = 0;
    h.cooldownUntil = 0;
  } else {
    h.failed++;
    h.lastError = error;
    h.consecutiveFails++;
    if (h.consecutiveFails >= BREAKER_THRESHOLD) h.cooldownUntil = Date.now() + BREAKER_COOLDOWN_MS;
  }
}

function isEnabled(p) {
  if (p.kind === 'disabled') return false;
  return providerSettings.has(p.id) ? providerSettings.get(p.id) : true;
}

function usable(id) {
  const p = PROVIDER_BY_ID.get(id);
  return Boolean(p && p.kind === 'chat' && isEnabled(p) && healthOf(id).cooldownUntil < Date.now());
}

async function callProvider(p, params, timeoutMs) {
  const url = new URL(p.path, AI_BASE_URL);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, String(v));
  if (url.href.length > MAX_URL_LENGTH) throw new Error('request is too long for a GET URL');

  let res;
  try {
    res = await fetch(url, { headers: { Accept: '*/*' }, signal: AbortSignal.timeout(timeoutMs) });
  } catch (err) {
    throw new Error(err.name === 'TimeoutError' ? `timed out after ${timeoutMs} ms` : 'network error');
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);

  const type = res.headers.get('content-type') || '';
  if (!/json|text\//i.test(type)) {
    const bytes = (await res.arrayBuffer()).byteLength;
    return { type, bytes, json: null, text: null };
  }
  const raw = await res.text();
  let json = null;
  try { json = JSON.parse(raw); } catch { /* plain text reply */ }
  if (json && typeof json === 'object' && json.status === false) {
    throw new Error(json.error || json.message || 'provider reported failure');
  }
  return { type, bytes: Buffer.byteLength(raw), json, text: json ? null : raw };
}

function extractText(out) {
  if (!out.json) return out.text;
  if (typeof out.json === 'string') return out.json;
  for (const k of ['result', 'response', 'answer', 'text', 'message', 'content']) {
    if (typeof out.json[k] === 'string') return out.json[k];
  }
  return null;
}

// Tries the chosen chat provider, then fallbacks. `say` receives every switch.
async function askText(preferred, prompt, say) {
  const order = [preferred, ...FALLBACK_ORDER.filter((id) => id !== preferred)]
    .filter(usable)
    .slice(0, MAX_PROVIDER_TRIES);
  if (!order.length) throw new Error('no enabled chat provider is available');

  const failures = [];
  for (const id of order) {
    const started = Date.now();
    try {
      const out = await callProvider(PROVIDER_BY_ID.get(id), { q: prompt }, AI_TIMEOUT_MS);
      const text = extractText(out);
      if (!text || !text.trim()) throw new Error('response had no usable text');
      record(id, true, Date.now() - started);
      return { provider: id, text };
    } catch (err) {
      record(id, false, Date.now() - started, err.message);
      failures.push(`${id}: ${err.message}`);
      if (say) await say('warn', `${id} failed (${err.message}); trying the next provider`);
    }
  }
  throw new Error(`all providers failed. ${failures.join('; ')}`);
}

// ---------- Agent briefs and file specs ----------
const planPrompt = (req) =>
  `Plan a small web app for this request in at most 6 short numbered items: data it stores, pages, API routes, validation. No code. Request: ${req}`;

const routesOf = (src) =>
  [...new Set([...src.matchAll(/app\.(?:get|post|put|patch|delete)\(\s*['"`]([^'"`]+)['"`]/g)].map((m) => m[1]))].slice(0, 15);

// Each file is requested on its own so the prompt stays under the GET URL limit.
const FILE_SPECS = [
  {
    path: 'schema.sql', agent: 'Database', kind: 'sql',
    brief: () => 'Write PostgreSQL CREATE TABLE IF NOT EXISTS statements for the data this app stores, with primary keys, foreign keys and indexes where useful. Never use DROP.',
  },
  {
    path: 'package.json', agent: 'Backend', kind: 'json',
    brief: () => 'Write package.json for a Node.js app with a "start" script that runs "node server.js", depending on express and pg. Output JSON only.',
  },
  {
    path: 'server.js', agent: 'Backend', kind: 'js',
    brief: () => 'Write server.js using CommonJS require only. It uses express and pg, reads process.env.DATABASE_URL and connects with ssl rejectUnauthorized true. At startup it reads schema.sql with fs and runs it. It serves the public folder as static files and exposes JSON REST routes under /api. It listens on process.env.PORT or 3000.',
  },
  {
    path: 'public/index.html', agent: 'Frontend', kind: 'html',
    brief: (c) => `Write a complete single-page HTML document with inline CSS and classic inline JavaScript. It must call these API routes with fetch: ${routesOf(c.built['server.js'] || '').join(', ') || 'none'}. Make it responsive, polished and working, with loading and error states.`,
  },
];

function filePrompt(ctx, spec, feedback) {
  return [
    'You are a senior engineer writing one file for a small web app.',
    `File: ${spec.path}.`,
    spec.brief(ctx),
    'Reply with exactly one fenced code block containing the complete file and nothing else.',
    ctx.plan ? `Plan: ${ctx.plan}` : '',
    `App request: ${ctx.request}`,
    feedback,
  ].filter(Boolean).join(' ');
}

function extractFile(text, spec) {
  let body;
  const open = text.indexOf('```');
  if (open === -1) {
    if (spec.kind !== 'json' && spec.kind !== 'sql') return { problem: 'reply had no code block' };
    body = text;
  } else {
    const lineEnd = text.indexOf('\n', open);
    if (lineEnd === -1) return { problem: 'reply had an unfinished code block' };
    const close = text.indexOf('```', lineEnd);
    if (close === -1) return { problem: 'reply was cut off before the closing code fence' };
    body = text.slice(lineEnd + 1, close);
  }
  body = body.trim();
  if (spec.kind === 'html') {
    const start = body.search(/<!doctype html|<html[\s>]/i);
    if (start === -1) return { problem: 'HTML document start tag is missing' };
    const end = body.toLowerCase().lastIndexOf('</html>');
    if (end === -1) return { problem: 'HTML was cut off before </html>' };
    body = body.slice(start, end + '</html>'.length);
  }
  if (!body) return { problem: 'code block was empty' };
  return { content: body + '\n' };
}

// QA: checks that compile or parse without running any generated code.
function qaFile(spec, content) {
  const errors = [];
  if (spec.kind === 'json') {
    try {
      const pkg = JSON.parse(content);
      if (!pkg.scripts?.start) errors.push('package.json needs a "start" script');
    } catch (e) {
      errors.push(`invalid JSON: ${e.message}`);
    }
  }
  if (spec.kind === 'js') {
    if (/^\s*import\s/m.test(content)) errors.push('server.js must use require(), not import');
    try { new Function(content); } catch (e) { errors.push(`JavaScript syntax error: ${e.message}`); }
  }
  if (spec.kind === 'sql') {
    if (!/create\s+table/i.test(content)) errors.push('no CREATE TABLE statement');
    if (/\bdrop\s+(table|database|schema)\b/i.test(content)) errors.push('contains DROP, which is not allowed');
  }
  if (spec.kind === 'html') {
    const opens = (content.match(/<script\b/gi) || []).length;
    const closes = (content.match(/<\/script>/gi) || []).length;
    if (opens !== closes) errors.push('unbalanced <script> tags');
    if (!/<title>\s*\S/i.test(content)) errors.push('missing <title>');
    for (const [, attrs, body] of content.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
      if (/\bsrc\s*=/i.test(attrs) || /\btype\s*=\s*["']?module/i.test(attrs)) continue;
      try { new Function(body); } catch (e) { errors.push(`JavaScript syntax error in a script tag: ${e.message}`); }
    }
  }
  return errors;
}

async function generateFile(ctx, spec) {
  let feedback = '';
  for (let attempt = 1; attempt <= MAX_FILE_ATTEMPTS; attempt++) {
    await ctx.say(spec.agent, 'info', attempt === 1 ? `Writing ${spec.path}` : `Repair ${attempt - 1} for ${spec.path}`);
    let r;
    try {
      r = await askText(ctx.preferred, filePrompt(ctx, spec, feedback), (lvl, msg) => ctx.say('API Integration', lvl, msg));
    } catch (err) {
      await ctx.say(spec.agent, 'error', `${spec.path}: ${err.message}`);
      return null; // every provider failed, so retrying will not help
    }
    const { content, problem } = extractFile(r.text, spec);
    if (problem) {
      feedback = `Problem: ${problem}. Reply with the complete file in one code block.`;
      await ctx.say('QA', 'warn', `${spec.path}: ${problem}`);
      continue;
    }
    const errors = qaFile(spec, content);
    if (errors.length) {
      feedback = `Fix these problems: ${errors.join('; ')}`.slice(0, 300);
      for (const e of errors) await ctx.say('QA', 'error', `${spec.path}: ${e}`);
      continue;
    }
    return content;
  }
  return null;
}

// Integration: fill in required dependencies and check that UI calls match server routes.
function ensureDeps(files) {
  const pkg = JSON.parse(files['package.json']);
  pkg.dependencies = { ...REQUIRED_DEPS, ...(pkg.dependencies || {}) };
  pkg.scripts = { start: 'node server.js', ...(pkg.scripts || {}) };
  return { ...files, 'package.json': JSON.stringify(pkg, null, 2) + '\n' };
}

function apiWarnings(files) {
  const routes = routesOf(files['server.js'] || '');
  const patterns = routes.map((r) => new RegExp('^' + r.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/:[^/]+/g, '[^/]+') + '$'));
  const calls = [...(files['public/index.html'] || '').matchAll(/fetch\(\s*[`'"](\/api[^`'"?]*)/g)]
    .map((m) => m[1].replace(/\$\{[^}]*\}/g, 'x'));
  return [...new Set(calls)]
    .filter((c) => !patterns.some((re) => re.test(c)))
    .map((c) => `Frontend calls ${c} but server.js has no matching route`);
}

// ---------- Saving files with checkpoints ----------
async function writeFiles(projectId, files, label) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const prev = await client.query('SELECT path, content FROM project_files WHERE project_id=$1', [projectId]);
    if (prev.rows.length) {
      const snap = Object.fromEntries(prev.rows.map((r) => [r.path, r.content]));
      await client.query(
        'INSERT INTO project_versions (id, project_id, label, files) VALUES ($1,$2,$3,$4)',
        [crypto.randomUUID(), projectId, label.slice(0, 200), JSON.stringify(snap)]
      );
      await client.query(
        `DELETE FROM project_versions WHERE project_id=$1 AND id NOT IN
           (SELECT id FROM project_versions WHERE project_id=$1 ORDER BY created_at DESC LIMIT $2)`,
        [projectId, MAX_VERSIONS]
      );
    }
    await client.query('DELETE FROM project_files WHERE project_id=$1', [projectId]);
    for (const [p, c] of Object.entries(files)) {
      await client.query('INSERT INTO project_files (project_id, path, content) VALUES ($1,$2,$3)', [projectId, p, c]);
    }
    await client.query('UPDATE projects SET updated_at=now() WHERE id=$1', [projectId]);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ---------- Build pipeline ----------
const running = new Set();

async function runBuild(projectId, request, preferred) {
  const buildId = crypto.randomUUID();
  running.add(projectId);
  const say = (agent, level, msg) => emit(projectId, buildId, agent, level, msg);
  const brief = request.slice(0, 500);
  let ok = false;

  try {
    await say('Orchestrator', 'info', `Request: "${brief.slice(0, 160)}"`);

    let plan = '';
    try {
      const r = await askText(preferred, planPrompt(brief), (lvl, msg) => say('API Integration', lvl, msg));
      plan = r.text.trim().slice(0, 300);
      await say('Orchestrator', 'info', `Plan ready (answered by ${r.provider})`);
    } catch (err) {
      await say('Orchestrator', 'warn', `No plan. Building from the request alone: ${err.message}`);
    }
    await say('Orchestrator', 'info', `Files in order: ${FILE_SPECS.map((s) => s.path).join(', ')}`);

    const ctx = { request: brief, plan, preferred, say, built: {} };
    for (const spec of FILE_SPECS) {
      const content = await generateFile(ctx, spec);
      if (content === null) {
        await say(spec.agent, 'error', `${spec.path} did not pass QA`);
        continue;
      }
      ctx.built[spec.path] = content;
      await say(spec.agent, 'info', `${spec.path} passed QA`);
    }

    const missing = FILE_SPECS.filter((s) => !ctx.built[s.path]).map((s) => s.path);
    if (missing.length) {
      await say('Integration', 'error', `Not saved: ${missing.join(', ')} did not pass. Your files are unchanged.`);
    } else {
      const files = ensureDeps(ctx.built);
      for (const w of apiWarnings(files)) await say('Integration', 'warn', w);
      await writeFiles(projectId, files, `Before: ${request.slice(0, 120)}`);
      await say('Integration', 'info', `Saved ${Object.keys(files).length} files. The previous version is under Versions.`);
      ok = true;
    }
  } catch (err) {
    await say('Orchestrator', 'error', `Build stopped: ${err.message}`).catch(() => {});
  } finally {
    running.delete(projectId);
    if (ok) await say('Deployment', 'info', 'Not connected yet. Deploy arrives in Phase 5.').catch(() => {});
    await setStatus(projectId, ok ? 'ready' : 'failed').catch((e) => console.error(e.message));
    publish(projectId, 'done', { ok });
  }
}

// ---------- Project views ----------
async function projectView(id) {
  const p = (await q('SELECT id, name, prompt, status, updated_at FROM projects WHERE id=$1', [id])).rows[0];
  const files = (await q('SELECT path, content FROM project_files WHERE project_id=$1 ORDER BY path', [id])).rows;
  const versions = (await q(
    'SELECT id, label, created_at FROM project_versions WHERE project_id=$1 ORDER BY created_at DESC', [id]
  )).rows;
  const { log } = await snapshot(id);
  return {
    id: p.id,
    name: p.name,
    prompt: p.prompt,
    status: p.status,
    updatedAt: p.updated_at,
    files: Object.fromEntries(files.map((f) => [f.path, f.content])),
    versions: versions.map((v) => ({ id: v.id, at: v.created_at, label: v.label })),
    deployments: [],
    lastLog: log,
  };
}

// ---------- Express ----------
const h = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

const requireProject = h(async (req, res, next) => {
  if (!isUuid(req.params.id)) return res.status(404).json({ error: 'Project not found' });
  const r = await q('SELECT id, name, prompt, status, updated_at FROM projects WHERE id=$1', [req.params.id]);
  if (!r.rows[0]) return res.status(404).json({ error: 'Project not found' });
  req.project = r.rows[0];
  next();
});

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.set({ 'X-Content-Type-Options': 'nosniff', 'X-Frame-Options': 'SAMEORIGIN', 'Referrer-Policy': 'no-referrer' });
  next();
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/api/healthz', (req, res) => res.json({ ok: true }));

app.get('/api/status', h(async (req, res) => {
  await q('SELECT 1');
  res.json({
    ok: true,
    db: 'connected',
    uptimeSeconds: Math.round(process.uptime()),
    providers: PROVIDERS.length,
    defaultChat: DEFAULT_CHAT,
    node: process.version,
  });
}));

// Providers
const providerView = (p) => {
  const h2 = healthOf(p.id);
  return {
    id: p.id,
    path: p.path,
    kind: p.kind,
    enabled: isEnabled(p),
    note: p.note || null,
    usable: usable(p.id),
    verified: Boolean(h2.lastOkAt),
    lastOkAt: h2.lastOkAt,
    lastLatencyMs: h2.lastLatencyMs,
    calls: h2.calls,
    ok: h2.ok,
    failed: h2.failed,
    lastError: h2.lastError,
    cooldownUntil: h2.cooldownUntil,
  };
};

app.get('/api/providers', (req, res) => {
  res.json({ providers: PROVIDERS.map(providerView), defaultChat: DEFAULT_CHAT, fallbacks: FALLBACK_ORDER });
});

app.patch('/api/providers/:id', h(async (req, res) => {
  const p = PROVIDER_BY_ID.get(req.params.id);
  if (!p) return res.status(404).json({ error: 'Unknown provider' });
  if (p.kind === 'disabled') return res.status(403).json({ error: 'This provider is locked and cannot be enabled' });
  if (typeof req.body?.enabled !== 'boolean') return res.status(400).json({ error: 'enabled must be true or false' });
  providerSettings.set(p.id, req.body.enabled);
  await q(
    'INSERT INTO provider_settings (id, enabled) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET enabled=EXCLUDED.enabled',
    [p.id, req.body.enabled]
  );
  res.json(providerView(p));
}));

app.post('/api/providers/:id/test', h(async (req, res) => {
  const p = PROVIDER_BY_ID.get(req.params.id);
  if (!p) return res.status(404).json({ error: 'Unknown provider' });
  if (p.kind === 'disabled') return res.status(403).json({ error: 'This provider is locked' });

  const started = Date.now();
  try {
    const out = await callProvider(p, p.example, p.kind === 'media' ? TEST_TIMEOUT_MS : 20_000);
    const text = extractText(out);
    if (p.kind !== 'media' && !text) throw new Error('response had no readable text');
    const latencyMs = Date.now() - started;
    record(p.id, true, latencyMs);
    const preview = text ?? (out.json ? JSON.stringify(out.json) : `[${out.bytes} bytes of ${out.type || 'binary'}]`);
    res.json({ ok: true, latencyMs, contentType: out.type, bytes: out.bytes, preview: preview.slice(0, 400) });
  } catch (err) {
    const latencyMs = Date.now() - started;
    record(p.id, false, latencyMs, err.message);
    res.status(502).json({ ok: false, latencyMs, error: err.message });
  }
}));

// Projects
app.get('/api/projects', h(async (req, res) => {
  const r = await q('SELECT id, name, prompt, status, updated_at FROM projects ORDER BY updated_at DESC');
  res.json({
    projects: r.rows.map((p) => ({ id: p.id, name: p.name, prompt: p.prompt, status: p.status, updatedAt: p.updated_at })),
  });
}));

app.post('/api/projects', h(async (req, res) => {
  const prompt = String(req.body?.prompt ?? '').trim().slice(0, 900);
  const name = String(req.body?.name ?? '').trim().slice(0, 80) || prompt.slice(0, 40) || 'Untitled app';
  const id = crypto.randomUUID();
  await q('INSERT INTO projects (id, name, prompt) VALUES ($1,$2,$3)', [id, name, prompt]);
  await q('INSERT INTO project_files (project_id, path, content) VALUES ($1,$2,$3)', [id, 'public/index.html', STARTER_HTML]);
  res.status(201).json(await projectView(id));
}));

app.get('/api/projects/:id', requireProject, h(async (req, res) => {
  res.json(await projectView(req.project.id));
}));

app.patch('/api/projects/:id', requireProject, h(async (req, res) => {
  const name = String(req.body?.name ?? '').trim().slice(0, 80);
  if (!name) return res.status(400).json({ error: 'Name is required' });
  await q('UPDATE projects SET name=$2, updated_at=now() WHERE id=$1', [req.project.id, name]);
  res.json(await projectView(req.project.id));
}));

app.delete('/api/projects/:id', requireProject, h(async (req, res) => {
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the build to finish first' });
  await q('DELETE FROM projects WHERE id=$1', [req.project.id]);
  res.status(204).end();
}));

// Files edited by hand in the Code tab
app.put('/api/projects/:id/file', requireProject, h(async (req, res) => {
  const { path: filePath, content } = req.body || {};
  if (!safePath(filePath)) return res.status(400).json({ error: 'Invalid file path' });
  if (typeof content !== 'string') return res.status(400).json({ error: 'content must be a string' });
  if (content.length > MAX_FILE_CHARS) return res.status(413).json({ error: 'File too large' });
  await q(
    `INSERT INTO project_files (project_id, path, content, updated_at) VALUES ($1,$2,$3,now())
     ON CONFLICT (project_id, path) DO UPDATE SET content=EXCLUDED.content, updated_at=now()`,
    [req.project.id, filePath, content]
  );
  await q('UPDATE projects SET updated_at=now() WHERE id=$1', [req.project.id]);
  res.json({ path: filePath, bytes: Buffer.byteLength(content) });
}));

// Live event stream: subscribes first, then sends a snapshot, so no event is missed
app.get('/api/projects/:id/events', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();

  if (!streams.has(pid)) streams.set(pid, new Set());
  streams.get(pid).add(res);
  const beat = setInterval(() => res.write(': ping\n\n'), 25_000);
  req.on('close', () => {
    clearInterval(beat);
    const set = streams.get(pid);
    if (set) {
      set.delete(res);
      if (!set.size) streams.delete(pid);
    }
  });

  const snap = await snapshot(pid);
  res.write(`event: snapshot\ndata: ${JSON.stringify(snap)}\n\n`);
}));

// Build (runs in the background; progress arrives through /events)
app.post('/api/projects/:id/build', requireProject, h(async (req, res) => {
  const p = req.project;
  if (running.has(p.id)) return res.status(409).json({ error: 'A build is already running for this project' });

  const text = String(req.body?.prompt ?? '').trim().slice(0, 600);
  if (!text) return res.status(400).json({ error: 'Prompt is required' });

  const chosen = PROVIDER_BY_ID.get(String(req.body?.provider || DEFAULT_CHAT));
  if (!chosen || chosen.kind !== 'chat') return res.status(400).json({ error: 'Choose a chat provider for builds' });

  const request = req.body?.mode === 'edit' && p.prompt
    ? `Change: ${text} (base app: ${p.prompt.slice(0, 300)})`
    : text;
  await q('UPDATE projects SET prompt=$2, status=$3, updated_at=now() WHERE id=$1', [p.id, request.slice(0, 900), 'building']);

  res.status(202).json({ started: true, request });
  runBuild(p.id, request, chosen.id).catch((err) => console.error('build crashed:', err));
}));

// Checkpoints
app.post('/api/projects/:id/restore/:versionId', requireProject, h(async (req, res) => {
  if (!isUuid(req.params.versionId)) return res.status(404).json({ error: 'Checkpoint not found' });
  const v = await q('SELECT label, files FROM project_versions WHERE id=$1 AND project_id=$2', [req.params.versionId, req.project.id]);
  if (!v.rows[0]) return res.status(404).json({ error: 'Checkpoint not found' });
  await writeFiles(req.project.id, v.rows[0].files, 'Before restore');
  res.json(await projectView(req.project.id));
}));

// Deployment is Phase 5: report that honestly instead of faking success
app.post('/api/projects/:id/deploy', requireProject, (req, res) => {
  res.status(501).json({ error: 'Deployment is not connected yet. It is planned for Phase 5 and needs a Vercel token.' });
});

// Preview: serves files from public/ only, so server-side code is never exposed
const MIME = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json', '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8' };

app.get(['/preview/:id', '/preview/:id/*'], h(async (req, res) => {
  const pid = req.params.id;
  if (!isUuid(pid)) return res.status(404).send('Not found');
  if (req.path === `/preview/${pid}`) return res.redirect(`/preview/${pid}/`);

  const rel = req.params[0] || '';
  const filePath = rel ? `public/${rel}` : 'public/index.html';
  const ext = path.extname(filePath).toLowerCase();
  if (!safePath(filePath) || !MIME[ext]) return res.status(404).send('Not found');

  const r = await q('SELECT content FROM project_files WHERE project_id=$1 AND path=$2', [pid, filePath]);
  if (!r.rows[0]) return res.status(404).send('Not found');
  res.type(MIME[ext]);
  res.set('Content-Security-Policy', "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: https:");
  res.send(r.rows[0].content);
}));

// Unknown API routes and errors return JSON without stack traces
app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));
app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ error: 'Invalid JSON body' });
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

// ---------- Startup ----------
async function start() {
  const schema = await fs.readFile(path.join(__dirname, 'schema.sql'), 'utf8');
  await pool.query(schema);

  const settings = await q('SELECT id, enabled FROM provider_settings');
  for (const row of settings.rows) providerSettings.set(row.id, row.enabled);

  // A build cannot survive a restart, so mark any stuck build as failed
  await q("UPDATE projects SET status='failed', updated_at=now() WHERE status='building'");

  app.listen(PORT, () => console.log(`BeraForge AI running at http://localhost:${PORT}`));
}

start().catch((err) => {
  console.error('Failed to start:', err.message);
  process.exit(1);
});
