CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE delayed_jobs (
  id bigserial PRIMARY KEY,
  priority integer DEFAULT 0,
  attempts integer DEFAULT 0,
  handler text,
  last_error text,
  run_at timestamp,
  locked_at timestamp,
  failed_at timestamp,
  locked_by text,
  queue text,
  created_at timestamp DEFAULT now(),
  updated_at timestamp DEFAULT now()
);
