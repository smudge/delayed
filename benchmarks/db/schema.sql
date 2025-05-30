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

CREATE TABLE benchmark_stats (
  id serial PRIMARY KEY,
  remaining integer NOT NULL
);

CREATE OR REPLACE FUNCTION decrement_counter_and_notify() RETURNS trigger AS $$
DECLARE
  remaining_jobs integer;
BEGIN
  UPDATE benchmark_stats
  SET remaining = remaining - 1
  WHERE id = 1;

  SELECT remaining INTO remaining_jobs FROM benchmark_stats WHERE id = 1;

  IF remaining_jobs <= 0 THEN
    PERFORM pg_notify('delayed_jobs_count', json_build_object(
      'at', now()::text,
      'remaining', remaining_jobs
    )::text);
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_when_empty
AFTER DELETE ON delayed_jobs
FOR EACH ROW
EXECUTE FUNCTION decrement_counter_and_notify();
