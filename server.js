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
const CHAT_URL = process.env.CHAT_URL || 'https://ch.at/v1/chat/completions';
const CHAT_TIMEOUT_MS = Number(process.env.CHAT_TIMEOUT_MS) || 120_000;
const MAX_STEPS = 15;        // tool calls allowed for one user message
const HISTORY_LIMIT = 30;    // recent messages sent to the model on each step
const MEMORY_LIMIT = 20;     // earlier user requests listed in the system prompt
const MAX_FILE_CHARS = 200_000;
const READ_LIMIT = 6000;
const MAX_VERSIONS = 10;
const FILE_PATH_RE = /^[A-Za-z0-9_][A-Za-z0-9._/-]{0,119}$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TOOLS = ['write_file', 'read_file', 'list_files', 'check_file', 'done'];
const AGENT_FOR_TOOL = { write_file: 'Builder', read_file: 'Builder', list_files: 'Builder', check_file: 'QA', done: 'Orchestrator' };
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

const SYSTEM = [
  'You are the Builder for BeraForge AI, an agent that writes a web project one file at a time.',
  'Reply with EXACTLY ONE JSON object per turn. No prose and no markdown fences.',
  'Tools:',
  '{"tool":"write_file","path":"public/index.html","content":"<complete file text>"}',
  '{"tool":"read_file","path":"public/index.html"}',
  '{"tool":"list_files"}',
  '{"tool":"check_file","path":"public/index.html"}',
  '{"tool":"done","summary":"one or two plain sentences for the user"}',
  'Rules:',
  '- Paths are relative. Use letters, digits, dot, underscore, hyphen and slash.',
  '- Write every file completely. Never use placeholders such as "rest of code here".',
  '- After writing a file, call check_file on it. Fix any reported problem with write_file.',
  '- HTML files must end with </html>. Put CSS and JavaScript inline unless you create separate files.',
  '- The preview shows only public/index.html. server.js, package.json and schema.sql are saved for a later phase and are not run yet.',
  '- Call done only after the requested work is written and checked.',
].join('\n');

const MIME = {
  '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml', '.txt': 'text/plain; charset=utf-8',
};

const safePath = (p) => typeof p === 'string' && FILE_PATH_RE.test(p) && !p.includes('..');
const isUuid = (s) => UUID_RE.test(String(s));
const now = () => new Date().toISOString();

// ---------- Database ----------
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: true }, max: 5 });
const q = (text, params) => pool.query(text, params);

// ---------- Live events (Server-Sent Events) ----------
const streams = new Map(); // projectId -> Set of SSE responses
const running = new Set(); // projects with an agent turn in progress

function publish(pid, type, data) {
  const set = streams.get(pid);
  if (!set) return;
  const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const r of set) r.write(msg);
}

const toMsg = (r) => ({ id: Number(r.id), agent: r.agent, text: r.display, at: r.created_at });

async function setStatus(pid, status) {
  await q('UPDATE projects SET status=$2, updated_at=now() WHERE id=$1', [pid, status]);
  publish(pid, 'status', { status });
}

// Saves a message. Rows with agent and display are shown in the chat. Rows with role user or assistant also go to the model.
async function save(pid, { role, agent = null, display = null, content = '' }) {
  const r = await q(
    'INSERT INTO messages (project_id, role, agent, display, content) VALUES ($1,$2,$3,$4,$5) RETURNING id, agent, display, created_at',
    [pid, role, agent, display, content]
  );
  if (agent && display) publish(pid, 'message', toMsg(r.rows[0]));
  return r.rows[0];
}

async function snapshot(pid) {
  const st = await q('SELECT status FROM projects WHERE id=$1', [pid]);
  const msgs = await q(
    `SELECT id, agent, display, created_at FROM (
       SELECT id, agent, display, created_at FROM messages
       WHERE project_id=$1 AND agent IS NOT NULL AND display IS NOT NULL
       ORDER BY id DESC LIMIT 200) t ORDER BY id`,
    [pid]
  );
  return { status: st.rows[0]?.status || 'draft', messages: msgs.rows.map(toMsg) };
}

// ---------- Chat provider (ch.at, OpenAI-compatible POST) ----------
async function chat(messages) {
  let lastErr;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(CHAT_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ messages }),
        signal: AbortSignal.timeout(CHAT_TIMEOUT_MS),
      });
      if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { retry: res.status === 429 || res.status >= 500 });
      const data = await res.json();
      const text = data?.choices?.[0]?.message?.content;
      if (typeof text !== 'string' || !text.trim()) throw new Error('empty reply');
      return text;
    } catch (err) {
      lastErr = err;
      const retry = err.retry || (err.name !== 'TimeoutError' && !err.message.startsWith('HTTP') && err.message !== 'empty reply');
      if (!retry) break;
      await new Promise((r) => setTimeout(r, 2000 * attempt));
    }
  }
  throw new Error(lastErr?.name === 'TimeoutError' ? 'the chat provider timed out' : lastErr.message);
}

// Reads one JSON object from the reply, even if the model added a code fence.
function parseAction(text) {
  const t = text.trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
  const start = t.indexOf('{');
  const end = t.lastIndexOf('}');
  if (start === -1 || end <= start) return null;
  try {
    const a = JSON.parse(t.slice(start, end + 1));
    return a && typeof a === 'object' && TOOLS.includes(a.tool) ? a : null;
  } catch {
    return null;
  }
}

// QA: checks that files compile or parse. Generated code is never executed here.
function checkContent(p, content) {
  const errors = [];
  if (p.endsWith('.json')) {
    try { JSON.parse(content); } catch (e) { errors.push(`invalid JSON: ${e.message}`); }
  } else if (p.endsWith('.js')) {
    if (/^\s*import\s/m.test(content)) errors.push('use require() instead of import in Node files');
    try { new Function(content); } catch (e) { errors.push(`syntax error: ${e.message}`); }
  } else if (p.endsWith('.html')) {
    if (!/<!doctype html|<html[\s>]/i.test(content)) errors.push('missing <html> start tag');
    if (!/<\/html>\s*$/i.test(content.trim())) errors.push('file does not end with </html>');
    const opens = (content.match(/<script\b/gi) || []).length;
    const closes = (content.match(/<\/script>/gi) || []).length;
    if (opens !== closes) errors.push('unbalanced <script> tags');
    for (const [, attrs, body] of content.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
      if (/\bsrc\s*=/i.test(attrs)) continue;
      try { new Function(body); } catch (e) { errors.push(`script syntax error: ${e.message}`); }
    }
  } else if (p.endsWith('.sql')) {
    if (!/create\s+table/i.test(content)) errors.push('no CREATE TABLE statement');
    if (/\bdrop\s+/i.test(content)) errors.push('DROP is not allowed');
  }
  return errors;
}

// ---------- Files and checkpoints ----------
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

async function upsertFile(pid, p, content) {
  await q(
    `INSERT INTO project_files (project_id, path, content, updated_at) VALUES ($1,$2,$3,now())
     ON CONFLICT (project_id, path) DO UPDATE SET content=EXCLUDED.content, updated_at=now()`,
    [pid, p, content]
  );
  await q('UPDATE projects SET updated_at=now() WHERE id=$1', [pid]);
}

// ---------- Tools ----------
const fail = (m) => ({ display: `Error: ${m}`, model: `Error: ${m}` });

async function runTool(pid, a) {
  if (a.tool === 'write_file') {
    if (!safePath(a.path)) return fail(`invalid path "${a.path}"`);
    if (typeof a.content !== 'string' || !a.content.trim()) return fail('content must be a non-empty string');
    if (a.content.length > MAX_FILE_CHARS) return fail('file is too large');
    await upsertFile(pid, a.path, a.content);
    const msg = `Wrote ${a.path} (${a.content.length} chars).`;
    return { display: msg, model: msg };
  }
  if (a.tool === 'read_file') {
    if (!safePath(a.path)) return fail(`invalid path "${a.path}"`);
    const r = await q('SELECT content FROM project_files WHERE project_id=$1 AND path=$2', [pid, a.path]);
    if (!r.rows[0]) return fail(`${a.path} does not exist`);
    const text = r.rows[0].content;
    return {
      display: `Read ${a.path} (${text.length} chars).`,
      model: text.length > READ_LIMIT ? `${text.slice(0, READ_LIMIT)}\n[truncated]` : text,
    };
  }
  if (a.tool === 'list_files') {
    const r = await q('SELECT path, length(content) AS chars FROM project_files WHERE project_id=$1 ORDER BY path', [pid]);
    const list = r.rows.map((f) => `${f.path} (${f.chars} chars)`).join(', ') || 'no files';
    return { display: `Files: ${list}`, model: list };
  }
  if (a.tool === 'check_file') {
    if (!safePath(a.path)) return fail(`invalid path "${a.path}"`);
    const r = await q('SELECT content FROM project_files WHERE project_id=$1 AND path=$2', [pid, a.path]);
    if (!r.rows[0]) return fail(`${a.path} does not exist`);
    const errors = checkContent(a.path, r.rows[0].content);
    if (!errors.length) return { display: `QA passed for ${a.path}.`, model: `QA passed for ${a.path}.` };
    const msg = `QA found ${errors.length} problem(s) in ${a.path}: ${errors.join('; ')}`;
    return { display: msg, model: msg };
  }
  return fail(`unknown tool ${a.tool}`);
}

// Compact form stored in history, so old file contents do not fill the model's context.
function compact(a) {
  if (a.tool === 'write_file') return { tool: 'write_file', path: a.path, chars: typeof a.content === 'string' ? a.content.length : 0 };
  if (a.tool === 'done') return { tool: 'done', summary: String(a.summary || '').slice(0, 300) };
  return { tool: a.tool, path: a.path };
}

// ---------- Agent turn ----------
async function modelMessages(pid) {
  const files = await q('SELECT path, length(content) AS chars FROM project_files WHERE project_id=$1 ORDER BY path', [pid]);
  const memory = await q(
    `SELECT display FROM messages WHERE project_id=$1 AND role='user' AND agent='You' ORDER BY id DESC LIMIT $2`,
    [pid, MEMORY_LIMIT]
  );
  const earlier = memory.rows.map((r) => r.display.slice(0, 160)).reverse();
  const system = [
    SYSTEM,
    `Files now: ${files.rows.map((f) => `${f.path} (${f.chars} chars)`).join(', ') || 'none'}`,
    earlier.length ? `Earlier user requests, oldest first:\n- ${earlier.join('\n- ')}` : '',
  ].filter(Boolean).join('\n\n');

  const hist = await q(
    `SELECT role, content FROM (
       SELECT id, role, content FROM messages WHERE project_id=$1 AND role IN ('user','assistant')
       ORDER BY id DESC LIMIT $2) t ORDER BY id`,
    [pid, HISTORY_LIMIT]
  );
  const rows = hist.rows.map((r) => ({ role: r.role, content: r.content }));
  while (rows.length && rows[0].role !== 'user') rows.shift();
  return [{ role: 'system', content: system }, ...rows];
}

async function runTurn(pid, text) {
  running.add(pid);
  let finished = false;
  try {
    await setStatus(pid, 'working');
    await snapshotFiles(pid, text);
    await save(pid, { role: 'user', agent: 'You', display: text, content: text });

    for (let step = 1; step <= MAX_STEPS; step++) {
      let reply;
      try {
        reply = await chat(await modelMessages(pid));
      } catch (err) {
        await save(pid, { role: 'system', agent: 'Orchestrator', display: `Chat provider failed: ${err.message}`, content: '' });
        break;
      }

      const action = parseAction(reply);
      if (!action) {
        await save(pid, {
          role: 'user', agent: 'QA',
          display: 'The reply was not a valid tool call. Asking the Builder again.',
          content: 'Your last reply was not one valid tool call. Reply with exactly one JSON object using one of the listed tools.',
        });
        continue;
      }

      await save(pid, { role: 'assistant', content: JSON.stringify(compact(action)) });

      if (action.tool === 'done') {
        const summary = String(action.summary || 'Done.').slice(0, 1000);
        await save(pid, {
          role: 'assistant', agent: 'Orchestrator', display: summary,
          content: JSON.stringify({ tool: 'done', summary }),
        });
        finished = true;
        break;
      }

      const result = await runTool(pid, action);
      await save(pid, {
        role: 'user', agent: AGENT_FOR_TOOL[action.tool], display: result.display,
        content: `Tool result (${action.tool}): ${result.model}`,
      });
    }

    if (!finished) {
      await save(pid, {
        role: 'system', agent: 'Orchestrator',
        display: `Stopped after ${MAX_STEPS} steps without finishing. Send another message to continue.`,
        content: '',
      });
    }
  } catch (err) {
    console.error('turn failed:', err);
    await save(pid, { role: 'system', agent: 'Orchestrator', display: `Turn stopped: ${err.message}`, content: '' }).catch(() => {});
  } finally {
    running.delete(pid);
    await setStatus(pid, finished ? 'ready' : 'failed').catch((e) => console.error(e.message));
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
  res.json({ ok: true, db: 'connected', chatUrl: CHAT_URL, uptimeSeconds: Math.round(process.uptime()), node: process.version });
}));

// Projects
app.get('/api/projects', h(async (req, res) => {
  const r = await q('SELECT id, name, status, updated_at FROM projects ORDER BY updated_at DESC');
  res.json({ projects: r.rows.map((p) => ({ id: p.id, name: p.name, status: p.status, updatedAt: p.updated_at })) });
}));

app.post('/api/projects', h(async (req, res) => {
  const name = String(req.body?.name ?? '').trim().slice(0, 80) || 'Untitled app';
  const id = crypto.randomUUID();
  await q('INSERT INTO projects (id, name) VALUES ($1,$2)', [id, name]);
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
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the agent to finish first' });
  await q('DELETE FROM projects WHERE id=$1', [req.project.id]);
  res.status(204).end();
}));

// Live events: subscribes first, then sends a snapshot. The client removes duplicate message IDs.
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

// Send a message to the Builder
app.post('/api/projects/:id/messages', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'The agent is still working on this project' });
  const text = String(req.body?.text ?? '').trim().slice(0, 2000);
  if (!text) return res.status(400).json({ error: 'Message is empty' });
  res.status(202).json({ started: true });
  runTurn(pid, text).catch((err) => console.error('turn crashed:', err));
}));

// Hand edits from the Code tab
app.put('/api/projects/:id/file', requireProject, h(async (req, res) => {
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the agent to finish first' });
  const { path: filePath, content } = req.body || {};
  if (!safePath(filePath)) return res.status(400).json({ error: 'Invalid file path' });
  if (typeof content !== 'string') return res.status(400).json({ error: 'content must be a string' });
  if (content.length > MAX_FILE_CHARS) return res.status(413).json({ error: 'File too large' });
  await upsertFile(req.project.id, filePath, content);
  res.json({ path: filePath, bytes: Buffer.byteLength(content) });
}));

// Checkpoints. Restoring saves the current files first, so a restore can be undone.
app.post('/api/projects/:id/restore/:versionId', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'Wait for the agent to finish first' });
  if (!isUuid(req.params.versionId)) return res.status(404).json({ error: 'Checkpoint not found' });
  const v = await q('SELECT files FROM project_versions WHERE id=$1 AND project_id=$2', [req.params.versionId, pid]);
  if (!v.rows[0]) return res.status(404).json({ error: 'Checkpoint not found' });
  await snapshotFiles(pid, 'Before restore');
  await replaceFiles(pid, v.rows[0].files);
  res.json(await projectView(pid));
}));

// Deploy is Phase 5. Report that honestly.
app.post('/api/projects/:id/deploy', requireProject, (req, res) => {
  res.status(501).json({ error: 'Deploy is not connected yet. It is planned for Phase 5.' });
});

// Preview: serves only files under public/. Server code and schema are never served.
app.get('/preview/:id', (req, res) => res.redirect(`/preview/${req.params.id}/`));
app.get('/preview/:id/*', h(async (req, res) => {
  const pid = req.params.id;
  if (!isUuid(pid)) return res.status(404).send('Not found');
  const rel = req.params[0] || 'index.html';
  const filePath = `public/${rel}`;
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
  // A turn cannot survive a restart, so mark any interrupted one as failed
  await q("UPDATE projects SET status='failed', updated_at=now() WHERE status='working'");
  app.listen(PORT, () => console.log(`BeraForge AI running on port ${PORT}`));
}

start().catch((err) => {
  console.error('Failed to start:', err.message);
  process.exit(1);
});
