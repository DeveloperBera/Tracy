'use strict';

const path = require('path');
const fs = require('fs/promises');
const crypto = require('crypto');
const express = require('express');

// ---------- Configuration ----------
const PORT = Number(process.env.PORT) || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data', 'projects.json');
const AI_BASE_URL = process.env.AI_BASE_URL || 'https://apis.keithsite.lol/';
const AI_TEXT_PATH = process.env.AI_TEXT_PATH || 'ai/gpt';
const AI_TIMEOUT_MS = Number(process.env.AI_TIMEOUT_MS) || 60_000;
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

// ---------- Persistence (JSON file; swap for Postgres later) ----------
let db = { projects: [] };

async function loadDb() {
  try {
    db = JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
  } catch (err) {
    if (err.code !== 'ENOENT') throw err;
  }
  db.projects ||= [];
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

// ---------- AI provider ----------
function pickText(raw) {
  try {
    const j = JSON.parse(raw);
    if (typeof j === 'string') return j;
    for (const k of ['result', 'response', 'answer', 'message', 'text', 'data', 'content']) {
      if (typeof j?.[k] === 'string') return j[k];
    }
  } catch {
    /* not JSON: use raw text */
  }
  return raw;
}

async function askAI(prompt) {
  const url = new URL(AI_TEXT_PATH, AI_BASE_URL);
  url.searchParams.set('q', prompt);
  let res;
  try {
    res = await fetch(url, {
      headers: { Accept: 'application/json, text/plain, */*' },
      signal: AbortSignal.timeout(AI_TIMEOUT_MS),
    });
  } catch (err) {
    throw new Error(err.name === 'TimeoutError' ? `timed out after ${AI_TIMEOUT_MS} ms` : err.message);
  }
  const raw = await res.text();
  if (!res.ok) throw new Error(`provider returned HTTP ${res.status}`);
  return pickText(raw);
}

const buildInstruction = (prompt) =>
  'You are a senior front-end engineer. Build the app described below as ONE complete, self-contained HTML document ' +
  'with inline CSS and inline JavaScript. Use no external libraries or network calls. Make it responsive, polished, ' +
  'dark-themed and fully working. Return only the code inside a single ```html code block, with no explanation. ' +
  'App request: ' + prompt;

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

function staticChecks(html) {
  const out = [];
  const count = (re) => (html.match(re) || []).length;
  if (count(/<script\b/gi) !== count(/<\/script>/gi)) out.push('Unbalanced <script> tags');
  if (count(/<style\b/gi) !== count(/<\/style>/gi)) out.push('Unbalanced <style> tags');
  if (!/<title>/i.test(html)) out.push('No <title> element');
  const external = html.match(/https?:\/\/[^\s"'<>)]+/gi) || [];
  if (external.length) out.push(`Uses ${external.length} external URL(s); they may not load in preview`);
  return out;
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
    headers: {
      Authorization: `Bearer ${VERCEL_TOKEN}`,
      'Content-Type': 'application/json',
    },
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
  ai: { endpoint: `${AI_BASE_URL}${AI_TEXT_PATH}` },
  deploy: { provider: VERCEL_TOKEN ? 'vercel' : null, configured: Boolean(VERCEL_TOKEN) },
  node: process.version,
}));

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

// Generation pipeline: Planner -> Frontend (AI) -> QA -> Integration
app.post('/api/projects/:id/generate', requireProject, async (req, res) => {
  const p = req.project;
  const prompt = String(req.body?.prompt ?? '').trim().slice(0, 1000);
  if (!prompt) return res.status(400).json({ error: 'Prompt is required' });

  const log = [];
  const say = (agent, msg, level = 'info') => log.push({ at: now(), agent, level, msg });
  const finish = (ok) => {
    p.status = ok ? 'ready' : 'failed';
    p.lastLog = log.slice(-200);
    p.updatedAt = now();
    persist();
    res.json({ ok, project: p });
  };

  say('Planner', `Received request: "${prompt.slice(0, 160)}"`);
  say('Frontend', `Requesting a single-file app from ${AI_BASE_URL}${AI_TEXT_PATH}`);

  let reply;
  try {
    reply = await askAI(buildInstruction(prompt));
  } catch (err) {
    say('Frontend', `AI request failed: ${err.message}`, 'error');
    return finish(false);
  }

  const html = extractHtml(reply);
  if (!html) {
    say('Frontend', 'Reply did not contain a complete HTML document', 'error');
    return finish(false);
  }
  say('Frontend', `Received ${html.length} characters of HTML`);

  const issues = staticChecks(html);
  if (issues.length) issues.forEach((i) => say('QA', i, 'warn'));
  else say('QA', 'Static checks passed');

  p.files['index.html'] = html;
  say('Integration', 'Saved index.html to the workspace');
  finish(true);
});

// Preview (sandboxed in the UI; served with a CSP that allows inline app code)
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
