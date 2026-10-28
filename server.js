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
  console.error('DATABASE_URL is not set. Add it in Render Environment or in your local .env file.');
  process.exit(1);
}
const KEITH_BASE = process.env.AI_BASE_URL || 'https://apis.keithsite.lol/';
const KEITH_PATHS = (process.env.KEITH_PATHS || '/ai/gpt,/ai/claudeai,/ai/gemini,/ai/deepseek,/keithai,/ai/mistral')
  .split(',').map((s) => s.trim()).filter(Boolean);
const KEITH_TIMEOUT_MS = Number(process.env.KEITH_TIMEOUT_MS) || 60_000;
const MAX_URL = 7000;                      // Keith accepts GET query strings only
const CHAT_URL = process.env.CHAT_URL || 'https://ch.at/v1/chat/completions';
const CHAT_TIMEOUT_MS = Number(process.env.CHAT_TIMEOUT_MS) || 180_000;
const MAX_BUILD_ATTEMPTS = 2;              // one try plus one repair
const HISTORY_MESSAGES = 8;                // chat lines sent to the model
const MAX_FILE_CHARS = 200_000;
const MAX_VERSIONS = 10;
const FILE_PATH = 'public/index.html';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FILE_PATH_RE = /^[A-Za-z0-9_][A-Za-z0-9._/-]{0,119}$/;
const GREETING = /^\s*(hi|hello|hey|yo|hola|thanks|thank you|ok|okay)([\s,!.?]+(bro|there|man|team))?[\s!.?]*$/i;
const STARTER_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New app</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#07080D;color:#EEF0F7;font-family:system-ui,sans-serif}</style>
</head>
<body><p>Send a message to the Builder to create this app.</p></body>
</html>`;

const BUILDER_RULES =
  'Write one complete HTML file with inline CSS and classic inline JavaScript. Use no external libraries, no ' +
  'imports, and no network calls. Make it responsive and fully working, with no placeholder buttons. Reply with ' +
  'only one ```html code block containing the whole document, ending with </html>.';

const routerRules = (hasApp) => [
  'You are the assistant in BeraForge AI, a chat tool where people build small web apps.',
  hasApp ? 'The project already has an app.' : 'The project has no app yet.',
  'If the user asks you to create, build, change, fix, style or add something to a web page or app, reply with one line that starts with BUILD: followed by a short summary of the work.',
  'Otherwise, reply conversationally in one to three short sentences. Do not write code in this reply.',
].join(' ');

const safePath = (p) => typeof p === 'string' && FILE_PATH_RE.test(p) && !p.includes('..');
const isUuid = (s) => UUID_RE.test(String(s));

// ---------- Database ----------
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: true }, max: 5 });
const q = (text, params) => pool.query(text, params);

// ---------- Live events ----------
const streams = new Map(); // projectId -> Set of SSE responses
const running = new Set(); // projects with a turn in progress

function publish(pid, type, data) {
  const set = streams.get(pid);
  if (!set) return;
  const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const r of set) r.write(msg);
}

const toMsg = (r) => ({ id: Number(r.id), agent: r.agent, text: r.display, at: r.created_at });

async function currentStatus(pid) {
  return (await q('SELECT status FROM projects WHERE id=$1', [pid])).rows[0]?.status || 'draft';
}

async function setStatus(pid, status) {
  await q('UPDATE projects SET status=$2, updated_at=now() WHERE id=$1', [pid, status]);
  publish(pid, 'status', { status });
}

async function save(pid, { role, agent, display, content = '' }) {
  const r = await q(
    'INSERT INTO messages (project_id, role, agent, display, content) VALUES ($1,$2,$3,$4,$5) RETURNING id, agent, display, created_at',
    [pid, role, agent, display, content]
  );
  publish(pid, 'message', toMsg(r.rows[0]));
}

const say = (pid, agent, text) => save(pid, { role: 'system', agent, display: text });

async function snapshot(pid) {
  const msgs = await q(
    `SELECT id, agent, display, created_at FROM (
       SELECT id, agent, display, created_at FROM messages
       WHERE project_id=$1 AND agent IS NOT NULL AND display IS NOT NULL
       ORDER BY id DESC LIMIT 200) t ORDER BY id`,
    [pid]
  );
  return { status: await currentStatus(pid), messages: msgs.rows.map(toMsg) };
}

// Chat lines for the model: only what the user and the assistant said.
async function recentChat(pid) {
  const r = await q(
    `SELECT agent, display FROM (
       SELECT id, agent, display FROM messages
       WHERE project_id=$1 AND agent IN ('You','Assistant')
       ORDER BY id DESC LIMIT $2) t ORDER BY id`,
    [pid, HISTORY_MESSAGES]
  );
  return r.rows.map((m) => ({ role: m.agent === 'You' ? 'user' : 'assistant', content: String(m.display).slice(0, 400) }));
}

// ---------- Model providers ----------
// Keith replies look like { status, creator, result }.
function keithText(raw) {
  let j;
  try { j = JSON.parse(raw); } catch { return raw; }
  if (typeof j === 'string') return j;
  if (j && j.status === false) throw new Error(j.error || 'provider reported failure');
  for (const k of ['result', 'response', 'answer', 'text', 'message']) {
    if (typeof j?.[k] === 'string') return j[k];
  }
  return null;
}

async function askKeith(prompt) {
  const errors = [];
  for (const p of KEITH_PATHS) {
    const url = new URL(p, KEITH_BASE);
    url.searchParams.set('q', prompt);
    if (url.href.length > MAX_URL) throw new Error('request is too long for a GET URL');
    try {
      const res = await fetch(url, { headers: { Accept: '*/*' }, signal: AbortSignal.timeout(KEITH_TIMEOUT_MS) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = keithText(await res.text());
      if (!text || !text.trim()) throw new Error('empty reply');
      return { text, via: `Keith ${p}` };
    } catch (err) {
      errors.push(`${p}: ${err.message}`);
    }
  }
  throw new Error(errors.join('; '));
}

async function askChAt(messages) {
  const res = await fetch(CHAT_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages }),
    signal: AbortSignal.timeout(CHAT_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  const text = data?.choices?.[0]?.message?.content;
  if (typeof text !== 'string' || !text.trim()) throw new Error('empty reply');
  return { text, via: 'ch.at' };
}

// Keith first. ch.at when every Keith endpoint fails or the prompt is too long for a GET URL.
async function ask({ system, history = [], user }) {
  const transcript = history.map((m) => `${m.role === 'user' ? 'User' : 'Assistant'}: ${m.content}`);
  const keithPrompt = [system, ...transcript, `User: ${user}`, 'Assistant:'].join('\n');
  try {
    return await askKeith(keithPrompt);
  } catch (keithErr) {
    const messages = [{ role: 'system', content: system }, ...history, { role: 'user', content: user }];
    try {
      return await askChAt(messages);
    } catch (chatErr) {
      throw new Error(`Keith: ${keithErr.message} | ch.at: ${chatErr.message}`);
    }
  }
}

// ---------- Extraction and QA ----------
function extractHtml(text) {
  let body = text;
  const fence = text.indexOf('```');
  if (fence !== -1) {
    const lineEnd = text.indexOf('\n', fence);
    if (lineEnd !== -1) {
      const close = text.indexOf('```', lineEnd);
      body = close === -1 ? text.slice(lineEnd + 1) : text.slice(lineEnd + 1, close);
    }
  }
  const start = body.search(/<!doctype html|<html[\s>]/i);
  if (start === -1) return null;
  const end = body.toLowerCase().lastIndexOf('</html>');
  if (end === -1) return null;
  return body.slice(start, end + '</html>'.length).trim() + '\n';
}

// Checks that inline scripts compile. Nothing generated is executed.
function qaHtml(html) {
  const errors = [];
  if (!/<title>\s*\S/i.test(html)) errors.push('missing <title>');
  const opens = (html.match(/<script\b/gi) || []).length;
  const closes = (html.match(/<\/script>/gi) || []).length;
  if (opens !== closes) errors.push('unbalanced <script> tags');
  for (const [, attrs, body] of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
    if (/\bsrc\s*=/i.test(attrs) || /\btype\s*=\s*["']?module/i.test(attrs)) continue;
    try { new Function(body); } catch (e) { errors.push(`JavaScript syntax error: ${e.message}`); }
  }
  return errors;
}

// ---------- Files and checkpoints ----------
async function currentFile(pid) {
  const r = await q('SELECT content FROM project_files WHERE project_id=$1 AND path=$2', [pid, FILE_PATH]);
  return r.rows[0]?.content ?? null;
}

async function snapshotFiles(pid, label) {
  const files = await q('SELECT path, content FROM project_files WHERE project_id=$1', [pid]);
  if (!files.rows.length) return;
  const snap = Object.fromEntries(files.rows.map((r) => [r.path, r.content]));
  await q('INSERT INTO project_versions (id, project_id, label, files) VALUES ($1,$2,$3,$4)',
    [crypto.randomUUID(), pid, label.slice(0, 200), JSON.stringify(snap)]);
  await q(
    `DELETE FROM project_versions WHERE project_id=$1 AND id NOT IN
       (SELECT id FROM project_versions WHERE project_id=$1 ORDER BY created_at DESC LIMIT $2)`,
    [pid, MAX_VERSIONS]
  );
}

async function upsertFile(pid, p, content) {
  await q(
    `INSERT INTO project_files (project_id, path, content, updated_at) VALUES ($1,$2,$3,now())
     ON CONFLICT (project_id, path) DO UPDATE SET content=EXCLUDED.content, updated_at=now()`,
    [pid, p, content]
  );
  await q('UPDATE projects SET updated_at=now() WHERE id=$1', [pid]);
}

async function replaceFiles(pid, files) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('DELETE FROM project_files WHERE project_id=$1', [pid]);
    for (const [p, c] of Object.entries(files)) {
      await client.query('INSERT INTO project_files (project_id, path, content) VALUES ($1,$2,$3)', [pid, p, c]);
    }
    await client.query('UPDATE projects SET updated_at=now() WHERE id=$1', [pid]);
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ---------- Build (returns true when the file was saved) ----------
async function buildApp(pid, request, current) {
  await say(pid, 'Builder', current ? 'Updating public/index.html…' : 'Writing public/index.html…');

  let html = null;
  let feedback = '';
  for (let attempt = 1; attempt <= MAX_BUILD_ATTEMPTS && !html; attempt++) {
    let prompt = request;
    if (current) prompt += `\n\nModify this existing file and keep everything the request does not change. Existing file:\n${current}`;
    if (feedback) prompt += `\n\nThe previous attempt had these problems, fix them: ${feedback}`;

    const reply = await ask({ system: BUILDER_RULES, user: prompt });
    await say(pid, 'Builder', `Reply from ${reply.via}`);

    const candidate = extractHtml(reply.text);
    const errors = candidate ? qaHtml(candidate) : ['the reply had no complete HTML document (it may have been cut off)'];
    if (!errors.length) {
      html = candidate;
      await say(pid, 'QA', 'Checks passed.');
    } else {
      feedback = errors.join('; ').slice(0, 300);
      await say(pid, 'QA', `Problems: ${feedback}`);
      if (attempt < MAX_BUILD_ATTEMPTS) await say(pid, 'Builder', 'Trying once more with the fixes…');
    }
  }

  if (!html) {
    await say(pid, 'Orchestrator', 'Not saved. Your previous file is unchanged. Try again with a shorter request.');
    return false;
  }

  await snapshotFiles(pid, request);
  await upsertFile(pid, FILE_PATH, html);
  await say(pid, 'Builder', `Saved ${FILE_PATH} (${html.length} chars). The preview is updated.`);
  // Summary line goes into chat memory so later messages know what exists
  await save(pid, {
    role: 'assistant', agent: 'Assistant',
    display: `Done. I ${current ? 'updated' : 'built'} the app: ${request.slice(0, 120)}.`, content: '',
  });
  return true;
}

// ---------- One user message ----------
async function runTurn(pid, text) {
  running.add(pid);
  let status = 'draft';
  try {
    const prior = await currentStatus(pid);
    status = prior === 'working' ? 'draft' : prior;
    await setStatus(pid, 'working');

    const current = await currentFile(pid);
    const hasApp = Boolean(current && current !== STARTER_HTML);

    // Chat history without the message just saved, which is sent separately
    const history = await recentChat(pid);
    const last = history[history.length - 1];
    if (last && last.role === 'user' && last.content === text.slice(0, 400)) history.pop();

    let route;
    try {
      route = await ask({ system: routerRules(hasApp), history, user: text });
    } catch (err) {
      await say(pid, 'Orchestrator', `I could not reach the model just now. Please try again. (${err.message.slice(0, 160)})`);
      return;
    }

    const out = route.text.trim();
    if (/^BUILD:/i.test(out) || /<!doctype|<html/i.test(out)) {
      const built = await buildApp(pid, text, hasApp ? current : null);
      status = built ? 'ready' : (hasApp ? 'ready' : 'failed');
    } else {
      const reply = out.replace(/```[\s\S]*?```/g, '').trim().slice(0, 1500) || 'Tell me what to build, or ask about the app you have.';
      await save(pid, { role: 'assistant', agent: 'Assistant', display: reply, content: '' });
    }
  } catch (err) {
    await say(pid, 'Orchestrator', `Stopped: ${err.message.slice(0, 200)}`).catch(() => {});
    status = status === 'working' ? 'failed' : status;
  } finally {
    running.delete(pid);
    await setStatus(pid, status).catch((e) => console.error(e.message));
  }
}

// ---------- Project views ----------
async function projectView(id) {
  const p = (await q('SELECT id, name, status FROM projects WHERE id=$1', [id])).rows[0];
  const files = (await q('SELECT path, content FROM project_files WHERE project_id=$1 ORDER BY path', [id])).rows;
  const versions = (await q(
    'SELECT id, label, created_at FROM project_versions WHERE project_id=$1 ORDER BY created_at DESC', [id]
  )).rows;
  return {
    id: p.id,
    name: p.name,
    status: p.status,
    files: Object.fromEntries(files.map((f) => [f.path, f.content])),
    versions: versions.map((v) => ({ id: v.id, at: v.created_at, label: v.label })),
  };
}

// ---------- Express ----------
const h = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

const requireProject = h(async (req, res, next) => {
  if (!isUuid(req.params.id)) return res.status(404).json({ error: 'Project not found' });
  const r = await q('SELECT id FROM projects WHERE id=$1', [req.params.id]);
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
  res.json({ ok: true, db: 'connected', keithPaths: KEITH_PATHS, chatFallback: CHAT_URL, uptimeSeconds: Math.round(process.uptime()), node: process.version });
}));

app.get('/api/projects', h(async (req, res) => {
  const r = await q('SELECT id, name, status, updated_at FROM projects ORDER BY updated_at DESC');
  res.json({ projects: r.rows.map((p) => ({ id: p.id, name: p.name, status: p.status, updatedAt: p.updated_at })) });
}));

app.post('/api/projects', h(async (req, res) => {
  const raw = String(req.body?.name ?? '').trim();
  const name = raw.length > 40 ? raw.slice(0, 40).trim() + '…' : raw || 'Untitled app';
  const id = crypto.randomUUID();
  await q('INSERT INTO projects (id, name) VALUES ($1,$2)', [id, name.slice(0, 80)]);
  await q('INSERT INTO project_files (project_id, path, content) VALUES ($1,$2,$3)', [id, FILE_PATH, STARTER_HTML]);
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
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  await q('DELETE FROM projects WHERE id=$1', [req.project.id]);
  res.status(204).end();
}));

// Live events: subscribe first, then send a snapshot
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

// Send a message. Every message is routed first: chat reply, or a build.
app.post('/api/projects/:id/messages', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'The Builder is still working on this project' });
  const text = String(req.body?.text ?? '').trim().slice(0, 1500);
  if (!text) return res.status(400).json({ error: 'Message is empty' });

  await save(pid, { role: 'user', agent: 'You', display: text, content: '' });

  if (GREETING.test(text)) {
    await save(pid, {
      role: 'assistant', agent: 'Assistant',
      display: 'Hi! Tell me what to build, or ask about the app you have.', content: '',
    });
    publish(pid, 'status', { status: await currentStatus(pid) });
    return res.status(202).json({ started: false });
  }

  res.status(202).json({ started: true });
  runTurn(pid, text).catch((err) => console.error('turn crashed:', err));
}));

// Hand edits from the Code tab
app.put('/api/projects/:id/file', requireProject, h(async (req, res) => {
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  const { path: filePath, content } = req.body || {};
  if (!safePath(filePath)) return res.status(400).json({ error: 'Invalid file path' });
  if (typeof content !== 'string') return res.status(400).json({ error: 'content must be a string' });
  if (content.length > MAX_FILE_CHARS) return res.status(413).json({ error: 'File too large' });
  await upsertFile(req.project.id, filePath, content);
  res.json({ path: filePath, bytes: Buffer.byteLength(content) });
}));

// Checkpoints. Restoring saves the current files first, so it can be undone.
app.post('/api/projects/:id/restore/:versionId', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  if (!isUuid(req.params.versionId)) return res.status(404).json({ error: 'Checkpoint not found' });
  const v = await q('SELECT files FROM project_versions WHERE id=$1 AND project_id=$2', [req.params.versionId, pid]);
  if (!v.rows[0]) return res.status(404).json({ error: 'Checkpoint not found' });
  await snapshotFiles(pid, 'Before restore');
  await replaceFiles(pid, v.rows[0].files);
  res.json(await projectView(pid));
}));

app.post('/api/projects/:id/deploy', requireProject, (req, res) => {
  res.status(501).json({ error: 'Deploy is not connected yet. It is planned for Phase 5.' });
});

// Preview: serves only files under public/
const MIME = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8',
};
app.get('/preview/:id', (req, res) => res.redirect(`/preview/${req.params.id}/`));
app.get('/preview/:id/*', h(async (req, res) => {
  const pid = req.params.id;
  if (!isUuid(pid)) return res.status(404).send('Not found');
  const filePath = `public/${req.params[0] || 'index.html'}`;
  const ext = path.extname(filePath).toLowerCase();
  if (!safePath(filePath) || !MIME[ext]) return res.status(404).send('Not found');
  const r = await q('SELECT content FROM project_files WHERE project_id=$1 AND path=$2', [pid, filePath]);
  if (!r.rows[0]) return res.status(404).send('Not found');
  res.type(MIME[ext]);
  res.set('Content-Security-Policy', "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: https:");
  res.send(r.rows[0].content);
}));

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
  await q("UPDATE projects SET status='failed', updated_at=now() WHERE status='working'");
  app.listen(PORT, () => console.log(`BeraForge AI running on port ${PORT}`));
}

start().catch((err) => {
  console.error('Failed to start:', err.message);
  process.exit(1);
});
