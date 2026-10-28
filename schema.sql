CREATE TABLE IF NOT EXISTS bf_projects (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  html TEXT NOT NULL DEFAULT '',
  summary TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bf_messages (
  id BIGSERIAL PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES bf_projects(id) ON DELETE CASCADE,
  role TEXT NOT NULL,
  agent TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bf_messages_project_idx ON bf_messages (project_id, id);

CREATE TABLE IF NOT EXISTS bf_versions (
  id UUID PRIMARY KEY,
  project_id UUID NOT NULL REFERENCES bf_projects(id) ON DELETE CASCADE,
  label TEXT NOT NULL,
  html TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bf_versions_project_idx ON bf_versions (project_id, created_at);
