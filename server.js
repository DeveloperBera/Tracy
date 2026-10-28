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
const KEITH_BASE = process.env.KEITH_BASE_URL || 'https://apis.keithsite.lol/';
const KEITH_PATHS = (process.env.KEITH_PATHS || '/ai/gpt,/ai/claudeai,/ai/gemini,/ai/deepseek,/keithai')
  .split(',').map((s) => s.trim()).filter(Boolean);
const CHAT_URL = process.env.CHAT_URL || 'https://ch.at/v1/chat/completions';
const KEITH_TIMEOUT_MS = 25_000;
const CHAT_TIMEOUT_MS = 120_000;
const KEITH_MAX_URL = 7000;      // Keith accepts GET query strings only
const HISTORY_MESSAGES = 12;     // chat messages sent to the model
const BUILD_ATTEMPTS = 2;        // one try plus one repair
const MAX_HTML_CHARS = 200_000;
const MAX_VERSIONS = 10;
const MAX_SUMMARY_CHARS = 800;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const STARTER_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>New project</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#07080D;color:#EEF0F7;font-family:system-ui,sans-serif}</style>
</head>
<body><p>Describe a page to build it here.</p></body>
</html>`;

const BUILD_RULES = [
  'You write one complete web page as a single HTML file.',
  'Use inline CSS and classic inline JavaScript only. No external libraries, no imports, no network calls.',
  'The page must be responsive and fully working. Do not leave placeholder buttons or dead links.',
  'Reply with only one ```html code block containing the whole document, starting with <!DOCTYPE html> and ending with </html>.',
].join(' ');

const CHAT_RULES = (summary) => [
  'You are the assistant in BeraForge AI, a tool where people describe small web pages and the Builder writes them.',
  'Answer the user directly and conversationally, in a few short sentences unless they ask for more.',
  'Do not write HTML in a chat reply.',
  summary
    ? `The project has a page built so far. Recent changes:\n${summary}`
    : 'The project has no page yet. If the user wants one, invite them to describe it.',
].join(' ');

// ---------- Routing (no model call needed) ----------
const BUILD_VERB = /\b(build|create|make|generate|change|update|fix|add|remove|move|restyle|redesign|rename)\b/i;
const BUILD_NOUN = /\b(page|website|site|app|dashboard|form|landing|counter|todo|game|calculator|blog|shop|store|menu|modal|navbar|header|footer|buttons?|colou?rs?|dark mode|light mode|font|layout|background|animation|design)\b/i;
const QUESTION_START = /^(what|why|who|when|where|explain|tell me|how does|is|are|can you explain|do you)\b/i;

function isBuildRequest(text) {
  if (QUESTION_START.test(text) && !BUILD_VERB.test(text)) return false;
  return BUILD_VERB.test(text) || BUILD_NOUN.test(text);
}

// ---------- Database ----------
const pool = new Pool({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: true }, max: 5 });
const q = (text, params) => pool.query(text, params);
const now = () => new Date().toISOString();

// ---------- Live events (Server-Sent Events) ----------
const streams = new Map(); // projectId -> Set of response objects
const running = new Set(); // projects with a turn in progress

function publish(pid, type, data) {
  const set = streams.get(pid);
  if (!set) return;
  const msg = `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const r of set) r.write(msg);
}

const toMessage = (r) => ({ id: Number(r.id), agent: r.agent, text: r.body, at: r.created_at });

async function setStatus(pid, status) {
  await q('UPDATE bf_projects SET status=$2, updated_at=now() WHERE id=$1', [pid, status]);
  publish(pid, 'status', { status });
}

async function save(pid, role, agent, body) {
  const r = await q(
    'INSERT INTO bf_messages (project_id, role, agent, body) VALUES ($1,$2,$3,$4) RETURNING id, agent, body, created_at',
    [pid, role, agent, body]
  );
  const m = toMessage(r.rows[0]);
  publish(pid, 'message', m);
  return m;
}

const log = (pid, agent, body) => save(pid, 'log', agent, body);

async function snapshot(pid) {
  const st = await q('SELECT status FROM bf_projects WHERE id=$1', [pid]);
  const msgs = await q(
    `SELECT id, agent, body, created_at FROM (
       SELECT id, agent, body, created_at FROM bf_messages
       WHERE project_id=$1 ORDER BY id DESC LIMIT 200) t ORDER BY id`,
    [pid]
  );
  return { status: st.rows[0]?.status || 'draft', messages: msgs.rows.map(toMessage) };
}

async function recentHistory(pid) {
  const r = await q(
    `SELECT role, body FROM (
       SELECT id, role, body FROM bf_messages
       WHERE project_id=$1 AND role IN ('user','assistant')
       ORDER BY id DESC LIMIT $2) t ORDER BY id`,
    [pid, HISTORY_MESSAGES]
  );
  return r.rows.map((m) => ({ role: m.role, content: m.body.slice(0, 1500) }));
}

// ---------- Model providers ----------
// Keith replies look like { status, creator, result }. Some endpoints may return plain text.
function keithResult(raw) {
  let j;
  try { j = JSON.parse(raw); } catch { return raw.trim() || null; }
  if (typeof j === 'string') return j.trim() || null;
  if (j && j.status === false) throw new Error(j.error || 'provider reported failure');
  return typeof j?.result === 'string' && j.result.trim() ? j.result : null;
}

// Keith takes one prompt in a GET query string, so the conversation is flattened into text.
function flatten(messages) {
  return messages.map((m) => `${m.role.toUpperCase()}: ${m.content}`).join('\n\n') + '\n\nASSISTANT:';
}

async function askKeith(messages) {
  const prompt = flatten(messages);
  const errors = [];
  for (const p of KEITH_PATHS) {
    const url = new URL(p, KEITH_BASE);
    url.searchParams.set('q', prompt);
    if (url.href.length > KEITH_MAX_URL) {
      errors.push('prompt too long for a GET request');
      break; // every path has the same length, so stop here
    }
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(KEITH_TIMEOUT_MS) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const text = keithResult(await res.text());
      if (!text) throw new Error('no result in reply');
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
  const data = await res.json().catch(() => null);
  const text = data?.choices?.[0]?.message?.content;
  if (typeof text !== 'string' || !text.trim()) throw new Error('no reply content');
  return { text, via: 'ch.at' };
}

// Chat: Keith first. Builds: ch.at first, because it has no URL length limit.
async function ask(messages, purpose) {
  const order = purpose === 'build' ? ['chat', 'keith'] : ['keith', 'chat'];
  const errors = [];
  for (const name of order) {
    try {
      return name === 'keith' ? await askKeith(messages) : await askChAt(messages);
    } catch (err) {
      errors.push(`${name === 'keith' ? 'Keith' : 'ch.at'}: ${err.message}`);
    }
  }
  throw new Error(errors.join(' | '));
}

// ---------- Extraction and QA ----------
function extractHtml(text) {
  let body = text;
  const open = text.indexOf('```');
  if (open !== -1) {
    const lineEnd = text.indexOf('\n', open);
    if (lineEnd !== -1) {
      const close = text.indexOf('```', lineEnd);
      body = close === -1 ? text.slice(lineEnd + 1) : text.slice(lineEnd + 1, close);
    }
  }
  const start = body.search(/<!doctype html|<html[\s>]/i);
  if (start === -1) return null;
  const endIdx = body.toLowerCase().lastIndexOf('</html>');
  if (endIdx === -1) return null;
  return body.slice(start, endIdx + '</html>'.length).trim() + '\n';
}

// Checks structure and that inline scripts compile. Generated code is never executed here.
function qaHtml(html) {
  const errors = [];
  if (html.length > MAX_HTML_CHARS) errors.push('the page is too large');
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

// ---------- Project data ----------
function addSummary(previous, request) {
  return `${previous}\n- ${request.slice(0, 120)}`.trim().slice(-MAX_SUMMARY_CHARS);
}

async function saveVersion(pid, label, html) {
  await q('INSERT INTO bf_versions (id, project_id, label, html) VALUES ($1,$2,$3,$4)',
    [crypto.randomUUID(), pid, label.slice(0, 200), html]);
  await q(
    `DELETE FROM bf_versions WHERE project_id=$1 AND id NOT IN
       (SELECT id FROM bf_versions WHERE project_id=$1 ORDER BY created_at DESC LIMIT $2)`,
    [pid, MAX_VERSIONS]
  );
}

async function projectView(id) {
  const p = (await q('SELECT id, name, status, html FROM bf_projects WHERE id=$1', [id])).rows[0];
  const versions = (await q(
    'SELECT id, label, created_at FROM bf_versions WHERE project_id=$1 ORDER BY created_at DESC', [id]
  )).rows;
  return {
    id: p.id,
    name: p.name,
    status: p.status,
    html: p.html,
    versions: versions.map((v) => ({ id: v.id, label: v.label, at: v.created_at })),
  };
}

// ---------- Build ----------
async function buildPage(pid, request, current) {
  let feedback = '';
  for (let attempt = 1; attempt <= BUILD_ATTEMPTS; attempt++) {
    await log(pid, 'Builder', attempt === 1 ? 'Writing index.html…' : 'Trying again with the fixes…');

    const parts = [`Request: ${request}`];
    parts.push(current
      ? `Change the existing page below. Keep everything the request does not change, and return the complete page.\n\nExisting page:\n${current}`
      : 'Create the page from scratch.');
    if (feedback) parts.push(`The previous attempt had these problems. Fix them: ${feedback}`);

    const reply = await ask(
      [{ role: 'system', content: BUILD_RULES }, { role: 'user', content: parts.join('\n\n') }],
      'build'
    );
    await log(pid, 'Builder', `Reply from ${reply.via}`);

    const html = extractHtml(reply.text);
    const problems = html ? qaHtml(html) : ['the reply had no complete HTML document (it may have been cut off)'];
    if (!problems.length) {
      await log(pid, 'QA', 'Checks passed.');
      return html;
    }
    feedback = problems.join('; ').slice(0, 300);
    await log(pid, 'QA', `Problems: ${feedback}`);
  }
  return null;
}

// ---------- One user message ----------
async function runTurn(pid, text, mode) {
  running.add(pid);
  let status = 'failed';
  try {
    await setStatus(pid, 'working');
    const project = (await q('SELECT html, summary FROM bf_projects WHERE id=$1', [pid])).rows[0];
    const hasPage = Boolean(project.html && project.html !== STARTER_HTML);
    const build = mode === 'build' || (mode === 'auto' && isBuildRequest(text));

    if (build) {
      const html = await buildPage(pid, text, hasPage ? project.html : null);
      if (html) {
        if (hasPage) await saveVersion(pid, `Before: ${text.slice(0, 120)}`, project.html);
        await q('UPDATE bf_projects SET html=$2, summary=$3, updated_at=now() WHERE id=$1',
          [pid, html, addSummary(project.summary, text)]);
        await log(pid, 'Builder', `Saved index.html (${html.length} characters). The preview is updated.`);
        await save(pid, 'assistant', 'Assistant', `Done. ${hasPage ? 'Updated' : 'Built'} the page: ${text.slice(0, 140)}`);
        status = 'ready';
      } else {
        await log(pid, 'Orchestrator', 'Not saved. Your previous page is unchanged. Try a shorter request.');
        status = hasPage ? 'ready' : 'failed';
      }
    } else {
      const history = await recentHistory(pid);
      const reply = await ask([{ role: 'system', content: CHAT_RULES(project.summary) }, ...history], 'chat');
      await save(pid, 'assistant', 'Assistant', reply.text.trim().slice(0, 4000));
      status = hasPage ? 'ready' : 'draft';
    }
  } catch (err) {
    await log(pid, 'Orchestrator', `Could not finish: ${err.message.slice(0, 240)}`).catch(() => {});
  } finally {
    running.delete(pid);
    await setStatus(pid, status).catch((e) => console.error(e.message));
  }
}

// ---------- Express ----------
const h = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

const requireProject = h(async (req, res, next) => {
  if (!UUID_RE.test(req.params.id)) return res.status(404).json({ error: 'Project not found' });
  const r = await q('SELECT id FROM bf_projects WHERE id=$1', [req.params.id]);
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
    ok: true, db: 'connected', keithPaths: KEITH_PATHS, chatUrl: CHAT_URL,
    uptimeSeconds: Math.round(process.uptime()), node: process.version,
  });
}));

app.get('/api/projects', h(async (req, res) => {
  const r = await q('SELECT id, name, status, updated_at FROM bf_projects ORDER BY updated_at DESC');
  res.json({ projects: r.rows.map((p) => ({ id: p.id, name: p.name, status: p.status, updatedAt: p.updated_at })) });
}));

app.post('/api/projects', h(async (req, res) => {
  const raw = String(req.body?.name ?? '').trim();
  const name = (raw || 'Untitled project').slice(0, 80);
  const id = crypto.randomUUID();
  await q('INSERT INTO bf_projects (id, name, html) VALUES ($1,$2,$3)', [id, name, STARTER_HTML]);
  res.status(201).json(await projectView(id));
}));

app.get('/api/projects/:id', requireProject, h(async (req, res) => {
  res.json(await projectView(req.project.id));
}));

app.patch('/api/projects/:id', requireProject, h(async (req, res) => {
  const name = String(req.body?.name ?? '').trim().slice(0, 80);
  if (!name) return res.status(400).json({ error: 'Name is required' });
  await q('UPDATE bf_projects SET name=$2, updated_at=now() WHERE id=$1', [req.project.id, name]);
  res.json(await projectView(req.project.id));
}));

app.delete('/api/projects/:id', requireProject, h(async (req, res) => {
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  await q('DELETE FROM bf_projects WHERE id=$1', [req.project.id]);
  res.status(204).end();
}));

// Live events: subscribe first, then send a snapshot. The client drops duplicate message IDs.
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

// Send a message. Prefix with /build or /chat to force a mode.
app.post('/api/projects/:id/messages', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'Still working on the previous message' });

  let text = String(req.body?.text ?? '').trim().slice(0, 1500);
  if (!text) return res.status(400).json({ error: 'Message is empty' });

  let mode = 'auto';
  const forced = text.match(/^\/(build|chat)\s+([\s\S]+)/i);
  if (forced) {
    mode = forced[1].toLowerCase();
    text = forced[2].trim();
  }

  await save(pid, 'user', 'You', text);
  res.status(202).json({ started: true });
  runTurn(pid, text, mode).catch((err) => console.error('turn crashed:', err));
}));

// Edits from the Code tab
app.put('/api/projects/:id/html', requireProject, h(async (req, res) => {
  if (running.has(req.project.id)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  const html = req.body?.html;
  if (typeof html !== 'string') return res.status(400).json({ error: 'html must be a string' });
  if (html.length > MAX_HTML_CHARS) return res.status(413).json({ error: 'The page is too large' });
  await q('UPDATE bf_projects SET html=$2, updated_at=now() WHERE id=$1', [req.project.id, html]);
  res.json({ ok: true, chars: html.length });
}));

// Checkpoints. The current page is saved before a restore, so a restore can be undone.
app.post('/api/projects/:id/restore/:versionId', requireProject, h(async (req, res) => {
  const pid = req.project.id;
  if (running.has(pid)) return res.status(409).json({ error: 'Wait for the Builder to finish first' });
  if (!UUID_RE.test(req.params.versionId)) return res.status(404).json({ error: 'Checkpoint not found' });
  const v = await q('SELECT html FROM bf_versions WHERE id=$1 AND project_id=$2', [req.params.versionId, pid]);
  if (!v.rows[0]) return res.status(404).json({ error: 'Checkpoint not found' });
  const current = (await q('SELECT html FROM bf_projects WHERE id=$1', [pid])).rows[0].html;
  await saveVersion(pid, 'Before restore', current);
  await q('UPDATE bf_projects SET html=$2, updated_at=now() WHERE id=$1', [pid, v.rows[0].html]);
  res.json(await projectView(pid));
}));

app.post('/api/projects/:id/deploy', requireProject, (req, res) => {
  res.status(501).json({ error: 'Deploy is not connected yet. It is planned for Phase 5.' });
});

// Preview: serves the page itself. The workspace also embeds it directly.
app.get('/preview/:id', requireProject, h(async (req, res) => {
  const r = await q('SELECT html FROM bf_projects WHERE id=$1', [req.project.id]);
  res.type('html');
  res.set('Content-Security-Policy', "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: blob: https:");
  res.send(r.rows[0].html);
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
  await q("UPDATE bf_projects SET status='failed', updated_at=now() WHERE status='working'");
  app.listen(PORT, () => console.log(`BeraForge AI running on port ${PORT}`));
}

start().catch((err) => {
  console.error('Failed to start:', err.message);
  process.exit(1);
});
