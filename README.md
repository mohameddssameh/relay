# Relay

[![CI](https://github.com/mohameddssameh/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/mohameddssameh/relay/actions/workflows/ci.yml)

A Postgres-backed job queue for Java, inspired by Sidekiq (Ruby) and BullMQ (Node).

## Why

Most Java shops already run Postgres. Standing up Redis (Sidekiq, BullMQ) or a
dedicated broker just to run background jobs is an extra piece of
infrastructure to operate, monitor, and pay for. Postgres 9.5+ gives you
everything a job queue actually needs at the storage layer:

- `SELECT ... FOR UPDATE SKIP LOCKED` for safe concurrent job claiming without
  a separate broker or polling library
- Transactions, so enqueueing a job can be part of the same transaction as
  the business change that triggered it
- `jsonb` for flexible job payloads with real query support if you ever need it

Relay leans into that: no broker, no extra moving parts, just Postgres and a
small worker loop.

## Stack

- Java 21
- Spring Boot 3.3+ (Spring JDBC only — no JPA/Hibernate, so `FOR UPDATE SKIP LOCKED`
  can be hand-written instead of fought with)
- PostgreSQL 16
- Flyway for schema migrations
- Thymeleaf + HTMX for the dashboard — server-rendered HTML, no JS build step
- JUnit 5 + Testcontainers for integration testing
- Gradle (Kotlin DSL)
- GitHub Actions for CI

## Architecture

```
                enqueue()
   caller ─────────────────────> RelayClient ──── INSERT ────┐
                                                               v
                                                    ┌─────────────────────┐
                                                    │       Postgres      │
                                                    │                     │
                                                    │  jobs   workers     │
                                                    │  dead_letter_jobs   │
                                                    └──┬────┬─────────┬───┘
                                    SELECT..FOR UPDATE │    │ INSERT..SELECT
                                       SKIP LOCKED     │    │ on exhausted
                                     (claim, complete,  │    │ retries
                                      requeue, retry)   │    │
                                                        v    ^
                                          heartbeat/reclaim  │
                                          every 5s / 10s     │
                                 ┌──────────────────────┐    │
                                 │      WorkerFleet      │────┘
                                 │  ┌─────────┐ ┌─────────┐   │
                                 │  │Worker A │ │Worker B │...│
                                 │  │(own id, │ │(own id, │   │
                                 │  │ own pool│ │ own pool│   │
                                 │  │ own sked)│ │ own sked)│  │
                                 │  └─────────┘ └─────────┘   │
                                 └────────────────────────────┘

                                 ┌────────────────────────────┐
                                 │  Dashboard (Thymeleaf+HTMX) │
                                 │  /dashboard                 │
                                 │  /dashboard/jobs[?status=X] │
                                 │  /dashboard/jobs/{id}       │
                                 │  /dashboard/dead-letter     │
                                 │  reads jobs/workers/         │
                                 │  dead_letter_jobs directly   │
                                 └────────────────────────────┘
```

`WorkerFleet` is the only Spring-managed piece of the worker subsystem — it reads
`relay.worker.instances` and constructs that many plain `Worker` objects, each with
its own UUID, its own bounded executor for handler-timeout enforcement, and its own
scheduled poll/heartbeat/reclaim cadence on a shared `ThreadPoolTaskScheduler`. Every
worker independently sweeps for stale peers (dead heartbeat > 30s) and reclaims their
`running` jobs back to `queued` — there's no leader election; `FOR UPDATE SKIP LOCKED`
on the `workers` row is what keeps concurrent sweeps from double-reclaiming the same
dead worker.

## Running locally

Start Postgres:

```bash
docker compose up -d
```

Run the app (applies Flyway migrations on startup, then polls for jobs every
2 seconds):

```bash
./gradlew bootRun
```

Run the tests (spins up its own ephemeral Postgres via Testcontainers — no
need for `docker compose up` first):

```bash
./gradlew test
```

## Dashboard

Run the app locally (`./gradlew bootRun`) and hit
[http://localhost:8080/dashboard](http://localhost:8080/dashboard) for live queue
counts, a filterable job list, per-job detail, and a dead-letter view with
one-click retry/delete. No auth — see Known limitations below.

## Status

Milestone 3: multiple `Worker` instances can safely share one queue
(`relay.worker.instances`), each tracked via a heartbeat with automatic
reclaim of a crashed worker's `running` jobs, plus a Thymeleaf+HTMX dashboard
for live visibility into the queue. No benchmarks, no deploy target, no
metrics endpoint yet — that's Milestone 4.

## Known limitations (Milestone 3)

- No priority queues, no scheduled/cron jobs, no per-type concurrency limits
  yet.
- Handler timeouts are enforced with cooperative cancellation
  (`Future.cancel(true)`), which only *requests* an interrupt — it can't force
  a CPU-bound or non-interruptible handler to actually stop. Such a handler
  keeps running in the background past its timeout (logged as a `WARN` if it
  hasn't stopped within 5s), even though the job itself has already been
  requeued or dead-lettered. Handlers doing long-running work should
  periodically check `Thread.currentThread().isInterrupted()` so they can
  cooperate with cancellation.
- The dashboard has no authentication or authorization at all — do not expose
  it to the internet as-is. It's a local/trusted-network debugging tool for
  now; auth is Milestone 4 or later.
- No per-worker job type routing yet — every worker polls every job type.
  There's no way to dedicate a worker (or a subset of a fleet) to a specific
  type, so a slow handler for one type competes for the same claim queue as
  every other type.
