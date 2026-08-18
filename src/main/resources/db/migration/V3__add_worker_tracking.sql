CREATE TABLE workers (
    id UUID PRIMARY KEY,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE jobs
    ADD COLUMN worker_id UUID REFERENCES workers(id) ON DELETE SET NULL;

CREATE INDEX idx_jobs_worker_id ON jobs (worker_id) WHERE worker_id IS NOT NULL;
