'use strict';

const path = require('path');
const fs = require('fs/promises');
const crypto = require('crypto');
const express = require('express');

// ---------- Configuration ----------
const PORT = Number(process.env.PORT) || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'projects.json');
const AI_BASE_URL = process.env.AI_BASE_URL || 'https://apis.keithsite.lol/';
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS) || 90_000;
const TEST_TIMEOUT_MS = 30_000;
const MAX_ATTEMPTS = 3;          // one generation plus up to two repairs
const MAX_PROVIDER_TRIES = 3;    // providers tried per request
const MAX_URL_LENGTH = 7000;     // the upstream API only accepts GET query strings
const BREAKER_THRESHOLD = 3;     // consecutive failures before a provider cools down
const BREAKER_COOLDOWN_MS = 120_000;
const MAX_VERSIONS = 10;
const DEFAULT_CHAT = 'ai_gpt';
const FALLBACK_ORDER = ['ai_claudeai', 'ai_gemini', 'ai_deepseek', 'keithai', 'ai_mistral'];
const VERCEL_TOKEN = process.env.VERCEL_TOKEN || '';
const VERCEL_TEAM_ID = process.env.VERCEL_TEAM_ID || '';
const FILE_NAME_RE = /^[A-Za-z0-9._-]{1,100}$/;
const MAX_FILE_CHARS = 300_000;
const STARTED_AT = Date.now();

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

// ---------- Persistence (JSON file; replace with Postgres later) ----------
let db = { projects: [], providerSettings: {} };

async function loadDb() {
  try {
    db = JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
  db.projects ||= [];
  db.providerSettings ||= {};
  for (const p of db.projects) {
    p.lastLog ||= [];
    p.versions ||= [];
    p.deployments ||= [];
    if (p.status === 'building') {
      p.status = 'failed';
      p.lastLog.push({ at: now(), agent: 'System', level: 'error', msg: 'Server restarted during the build' });
    }
  }
}

let writeQueue = Promise.resolve();
function persist() {
  writeQueue = writeQueue
    .then(async () => {
      await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
      const tmp = `${DATA_FILE}.tmp`;
      await fs.writeFile(tmp, JSON.stringify(db, null, 2));
      await fs.rename(tmp, DATA_FILE);
    })
    .catch((err) => console.error('persist failed:', err.message));
  return writeQueue;
}

const now = () => new Date().toISOString();
const findProject = (id) => db.projects.find((p) => p.id === id);
const summary = (p) => ({
  id: p.id,
  name: p.name,
  prompt: p.prompt,
  status: p.status,
  updatedAt: p.updatedAt,
  latestDeploymentUrl: p.deployments[0]?.url || null,
});

function requireProject(req, res, next) {
  const p = findProject(req.params.id);
  if (!p) return res.status(404).json({ error: 'Project not found' });
  req.project = p;
  next();
}

// ---------- Live events (Server-Sent Events) ----------
const streams = new Map(); // projectId -> Set of response objects

function publish(p, event) {
  const set = streams.get(p.id);
  if (!set) return;
  const data = `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
  for (const res of set) res.write(data);
}

function emit(p, agent, level, msg) {
  const entry = { at: now(), agent, level, msg };
  p.lastLog = [...p.lastLog, entry].slice(-200);
  publish(p, { type: 'log', ...entry });
}

function setStatus(p, status) {
  p.status = status;
  p.updatedAt = now();
  publish(p, { type: 'status', status });
  persist();
}

// ---------- Provider health and calls ----------
const health = new Map(); // in memory only; resets on restart

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
  return db.providerSettings[p.id]?.enabled ?? true;
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

// Tries the preferred chat provider, then fallbacks. Every switch is reported through emit().
async function askText(preferred, prompt, emitFn) {
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
      if (emitFn) emitFn('API', 'warn', `${id} failed (${err.message}); trying the next provider`);
    }
  }
  throw new Error(`all providers failed. ${failures.join('; ')}`);
}

// ---------- Prompts, extraction, QA ----------
const plannerPrompt = (req) =>
  `Plan a single-file web app for this request. Reply with 5 to 8 short numbered items covering layout, features, state and validation. No code. Request: ${req}`;

const builderPrompt = (req, plan) =>
  [
    'You are a senior front-end engineer.',
    'Build the app below as ONE complete HTML document with inline CSS and classic inline JavaScript.',
    'Use no external libraries, no network calls, and no import or export statements.',
    'Make it responsive and fully working. Do not use placeholder buttons.',
    'Return only one ```html code block containing the full document, ending with </html>.',
    plan ? `Plan: ${plan}` : '',
    `App request: ${req}`,
  ].filter(Boolean).join(' ');

function extractHtml(text) {
  const fence = text.match(/```(?:html)?\s*([\s\S]*?)```/i);
  let html = fence ? fence[1] : text;
  const start = html.search(/<!doctype html|<html[\s>]/i);
  if (start === -1) return null;
  html = html.slice(start);
  const end = html.toLowerCase().lastIndexOf('</html>');
  if (end === -1) return null;
  return html.slice(0, end + '</html>'.length).trim();
}

function qaCheck(html) {
  const errors = [];
  const warnings = [];
  const opens = (html.match(/<script\b/gi) || []).length;
  const closes = (html.match(/<\/script>/gi) || []).length;
  if (opens !== closes) errors.push('unbalanced <script> tags');
  if (!/<title>\s*\S/i.test(html)) warnings.push('no page title');

  [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)].forEach(([, attrs, body], i) => {
    if (/\bsrc\s*=/i.test(attrs)) {
      warnings.push(`script ${i + 1} loads an external file`);
      return;
    }
    if (/\btype\s*=\s*["']?module/i.test(attrs)) {
      warnings.push(`script ${i + 1} is a module and was not syntax-checked`);
      return;
    }
    try {
      new Function(body); // compiles only; nothing is executed
    } catch (err) {
      errors.push(`JavaScript syntax error in script ${i + 1}: ${err.message}`);
    }
  });

  const external = html.match(/https?:\/\/[^\s"'<>)]+/gi) || [];
  if (external.length) warnings.push(`${external.length} external URL(s) may not load in the preview`);
  return { errors, warnings };
}

// ---------- Build pipeline: Planner -> Frontend -> QA -> repair -> Integration ----------
const running = new Set();

async function runBuild(p, request, preferred) {
  running.add(p.id);
  const before = p.lastRequest || 'Starter page';
  const previousHtml = p.files['index.html'];
  p.lastLog = [];
  setStatus(p, 'building');

  const note = (agent, level, msg) => emit(p, agent, level, msg);
  let ok = false;

  try {
    note('Planner', 'info', `Request: "${request.slice(0, 160)}"`);
    let plan = '';
    try {
      const r = await askText(preferred, plannerPrompt(request), note);
      plan = r.text.slice(0, 600);
      note('Planner', 'info', `Plan ready (answered by ${r.provider})`);
    } catch (err) {
      note('Planner', 'warn', `No plan, building from the request alone: ${err.message}`);
    }

    let prompt = builderPrompt(request, plan);
    let html = null;
    let qa = { errors: [], warnings: [] };

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      note('Frontend', 'info', attempt === 1 ? 'Generating application' : `Repair attempt ${attempt - 1} of ${MAX_ATTEMPTS - 1}`);
      const r = await askText(preferred, prompt, note);
      note('Frontend', 'info', `Reply from ${r.provider}`);

      const candidate = extractHtml(r.text);
      if (candidate) {
        html = candidate;
        qa = qaCheck(html);
      } else {
        qa = { errors: ['reply had no complete HTML document (it may have been cut off)'], warnings: [] };
      }
      qa.errors.forEach((e) => note('QA', 'error', e));
      qa.warnings.forEach((w) => note('QA', 'warn', w));

      if (qa.errors.length === 0) {
        note('QA', 'info', 'Checks passed');
        break;
      }
      // Repairs regenerate with the error list; the GET URL cannot carry the broken HTML.
      prompt = `${builderPrompt(request, plan)} The previous attempt had these problems, fix them all: ${qa.errors.join('; ').slice(0, 400)}`;
    }

    if (html && qa.errors.length === 0) {
      p.versions = [
        { id: crypto.randomUUID(), at: now(), label: `Before: ${before.slice(0, 120)}`, html: previousHtml },
        ...p.versions,
      ].slice(0, MAX_VERSIONS);
      p.files['index.html'] = html;
      p.lastRequest = request;
      note('Integration', 'info', 'Saved index.html. The previous version is under Versions.');
      ok = true;
    } else {
      note('Integration', 'error', 'Not saved: QA errors remain. Your files are unchanged.');
    }
  } catch (err) {
    note('Frontend', 'error', `Build stopped: ${err.message}`);
  } finally {
    running.delete(p.id);
    setStatus(p, ok ? 'ready' : 'failed');
    publish(p, { type: 'done', ok });
  }
}

// ---------- Vercel ----------
function vercelUrl(pathname) {
  const u = new URL(`https://api.vercel.com${pathname}`);
  if (VERCEL_TEAM_ID) u.searchParams.set('teamId', VERCEL_TEAM_ID);
  return u;
}

async function vercel(pathname, init = {}) {
  const res = await fetch(vercelUrl(pathname), {
    ...init,
    headers: { Authorization: `Bearer ${VERCEL_TOKEN}`, 'Content-Type': 'application/json' },
    signal: AbortSignal.timeout(30_000),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error?.message || `Vercel returned HTTP ${res.status}`);
    err.status = res.status;
    throw err;
  }
  return data;
}

const slug = (s) =>
  String(s).toLowerCase().replace(/[^a-z0-9-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 90) || 'beraforge-app';

// ---------- App ----------
const app = express();
app.use(express.json({ limit: '2mb' }));
app.use((req, res, next) => {
  res.set({
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'SAMEORIGIN',
    'Referrer-Policy': 'no-referrer',
  });
  next();
});

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/api/healthz', (req, res) => res.json({ ok: true }));

app.get('/api/status', (req, res) => res.json({
  ok: true,
  uptimeSeconds: Math.round((Date.now() - STARTED_AT) / 1000),
  projects: db.projects.length,
  providers: PROVIDERS.length,
  defaultChat: DEFAULT_CHAT,
  deploy: { provider: VERCEL_TOKEN ? 'vercel' : null, configured: Boolean(VERCEL_TOKEN) },
  node: process.version,
}));

// Providers
const providerView = (p) => {
  const h = healthOf(p.id);
  return {
    id: p.id,
    path: p.path,
    kind: p.kind,
    enabled: isEnabled(p),
    note: p.note || null,
    params: Object.keys(p.example),
    usable: usable(p.id),
    verified: Boolean(h.lastOkAt),
    lastOkAt: h.lastOkAt,
    lastLatencyMs: h.lastLatencyMs,
    calls: h.calls,
    ok: h.ok,
    failed: h.failed,
    lastError: h.lastError,
    cooldownUntil: h.cooldownUntil,
  };
};

app.get('/api/providers', (req, res) => {
  res.json({ providers: PROVIDERS.map(providerView), defaultChat: DEFAULT_CHAT, fallbacks: FALLBACK_ORDER });
});

app.patch('/api/providers/:id', (req, res) => {
  const p = PROVIDER_BY_ID.get(req.params.id);
  if (!p) return res.status(404).json({ error: 'Unknown provider' });
  if (p.kind === 'disabled') return res.status(403).json({ error: 'This provider is locked and cannot be enabled' });
  if (typeof req.body?.enabled !== 'boolean') return res.status(400).json({ error: 'enabled must be true or false' });
  db.providerSettings[p.id] = { enabled: req.body.enabled };
  persist();
  res.json(providerView(p));
});

app.post('/api/providers/:id/test', async (req, res) => {
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
});

// Projects
app.get('/api/projects', (req, res) => {
  res.json({ projects: db.projects.map(summary) });
});

app.post('/api/projects', (req, res) => {
  const prompt = String(req.body?.prompt ?? '').trim().slice(0, 1000);
  const rawName = String(req.body?.name ?? '').trim().slice(0, 80);
  const name = rawName || (prompt ? prompt.slice(0, 40) : 'Untitled app');
  const ts = now();
  const project = {
    id: crypto.randomUUID(),
    name,
    prompt,
    status: 'draft',
    createdAt: ts,
    updatedAt: ts,
    files: { 'index.html': STARTER_HTML },
    lastLog: [],
    versions: [],
    deployments: [],
  };
  db.projects.unshift(project);
  persist();
  res.status(201).json(project);
});

app.get('/api/projects/:id', requireProject, (req, res) => res.json(req.project));

app.patch('/api/projects/:id', requireProject, (req, res) => {
  const name = String(req.body?.name ?? '').trim().slice(0, 80);
  if (!name) return res.status(400).json({ error: 'Name is required' });
  req.project.name = name;
  req.project.updatedAt = now();
  persist();
  res.json(req.project);
});

app.delete('/api/projects/:id', requireProject, (req, res) => {
  db.projects = db.projects.filter((p) => p.id !== req.project.id);
  persist();
  res.status(204).end();
});

// Live event stream: sends a snapshot on connect, then live updates
app.get('/api/projects/:id/events', requireProject, (req, res) => {
  const p = req.project;
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.flushHeaders();
  res.write(`event: snapshot\ndata: ${JSON.stringify({ status: p.status, log: p.lastLog })}\n\n`);

  if (!streams.has(p.id)) streams.set(p.id, new Set());
  streams.get(p.id).add(res);
  const beat = setInterval(() => res.write(': ping\n\n'), 25_000);

  req.on('close', () => {
    clearInterval(beat);
    const set = streams.get(p.id);
    if (set) {
      set.delete(res);
      if (!set.size) streams.delete(p.id);
    }
  });
});

// Build
app.post('/api/projects/:id/generate', requireProject, (req, res) => {
  const p = req.project;
  if (running.has(p.id)) return res.status(409).json({ error: 'A build is already running for this project' });

  const text = String(req.body?.prompt ?? '').trim();
  if (!text) return res.status(400).json({ error: 'Prompt is required' });

  const preferred = String(req.body?.provider || DEFAULT_CHAT);
  const chosen = PROVIDER_BY_ID.get(preferred);
  if (!chosen || chosen.kind !== 'chat') return res.status(400).json({ error: 'Choose a chat provider for builds' });

  const isEdit = req.body?.mode === 'edit' && p.prompt;
  const request = (isEdit ? `${p.prompt} | Change: ${text}` : text).slice(0, 1000);
  p.prompt = request;

  res.status(202).json({ started: true, request });
  runBuild(p, request, preferred).catch((err) => console.error('build crashed:', err));
});

// Checkpoints
app.post('/api/projects/:id/restore/:versionId', requireProject, (req, res) => {
  const p = req.project;
  const idx = p.versions.findIndex((v) => v.id === req.params.versionId);
  if (idx === -1) return res.status(404).json({ error: 'Checkpoint not found' });
  const [version] = p.versions.splice(idx, 1);
  p.versions.unshift({ id: crypto.randomUUID(), at: now(), label: 'Before restore', html: p.files['index.html'] });
  p.versions = p.versions.slice(0, MAX_VERSIONS);
  p.files['index.html'] = version.html;
  p.updatedAt = now();
  persist();
  res.json(p);
});

// Files
app.put('/api/projects/:id/files/:name', requireProject, (req, res) => {
  const { name } = req.params;
  if (!FILE_NAME_RE.test(name)) return res.status(400).json({ error: 'Invalid file name' });
  const content = req.body?.content;
  if (typeof content !== 'string') return res.status(400).json({ error: 'content must be a string' });
  if (content.length > MAX_FILE_CHARS) return res.status(413).json({ error: 'File too large' });
  req.project.files[name] = content;
  req.project.updatedAt = now();
  persist();
  res.json({ name, bytes: Buffer.byteLength(content) });
});

// Preview
app.get('/preview/:id/:file?', (req, res) => {
  const p = findProject(req.params.id);
  if (!p) return res.status(404).send('Not found');
  const name = req.params.file || 'index.html';
  if (!FILE_NAME_RE.test(name) || !Object.prototype.hasOwnProperty.call(p.files, name)) {
    return res.status(404).send('Not found');
  }
  const ext = path.extname(name).toLowerCase();
  res.type(ext === '.css' ? 'text/css' : ext === '.js' ? 'text/javascript' : 'text/html; charset=utf-8');
  res.set('Content-Security-Policy', "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: https:");
  res.send(p.files[name]);
});

// Deployment (Vercel)
app.post('/api/projects/:id/deploy', requireProject, async (req, res) => {
  if (!VERCEL_TOKEN) {
    return res.status(503).json({
      error: 'Deployment is not configured',
      setup: 'Set VERCEL_TOKEN (and VERCEL_TEAM_ID if needed) in the server environment, then restart.',
    });
  }
  const p = req.project;
  try {
    const dep = await vercel('/v13/deployments', {
      method: 'POST',
      body: JSON.stringify({
        name: slug(p.name),
        target: 'production',
        files: Object.entries(p.files).map(([file, data]) => ({ file, data })),
      }),
    });
    const record = {
      id: dep.id,
      url: dep.url ? `https://${dep.url}` : null,
      state: dep.readyState || 'QUEUED',
      createdAt: now(),
    };
    p.deployments = [record, ...p.deployments].slice(0, 20);
    p.updatedAt = now();
    persist();
    res.status(201).json(record);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

app.get('/api/projects/:id/deployments/:depId', requireProject, async (req, res) => {
  if (!VERCEL_TOKEN) return res.status(503).json({ error: 'Deployment is not configured' });
  const record = req.project.deployments.find((d) => d.id === req.params.depId);
  if (!record) return res.status(404).json({ error: 'Deployment not found' });
  try {
    const dep = await vercel(`/v13/deployments/${encodeURIComponent(record.id)}`);
    record.state = dep.readyState || record.state;
    if (dep.url) record.url = `https://${dep.url}`;
    persist();
    res.json(record);
  } catch (err) {
    res.status(502).json({ error: err.message });
  }
});

// Unknown API routes and errors return JSON without stack traces
app.use('/api', (req, res) => res.status(404).json({ error: 'Not found' }));

app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') return res.status(400).json({ error: 'Invalid JSON body' });
  console.error(err);
  res.status(500).json({ error: 'Internal server error' });
});

loadDb()
  .then(() => {
    app.listen(PORT, () => console.log(`BeraForge AI running at http://localhost:${PORT}`));
  })
  .catch((err) => {
    console.error('Failed to start:', err);
    process.exit(1);
  });
