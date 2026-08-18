ALTER TABLE jobs
    ADD COLUMN attempts INT NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT,
    ADD COLUMN last_failed_at TIMESTAMPTZ;

CREATE TABLE dead_letter_jobs (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    last_failed_at TIMESTAMPTZ,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
