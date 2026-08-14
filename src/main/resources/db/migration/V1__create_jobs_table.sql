CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_check CHECK (status IN ('queued', 'running', 'completed', 'failed'))
);

CREATE INDEX idx_jobs_status_run_at ON jobs (status, run_at) WHERE status = 'queued';
