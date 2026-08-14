# Relay

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
- JUnit 5 + Testcontainers for integration testing
- Gradle (Kotlin DSL)
- GitHub Actions for CI

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

## Status

Milestone 1: enqueue a job, have a worker claim and run it, watch the status
flip to `completed` in Postgres. No retries, no scheduling UI, no delayed
jobs yet — that's later milestones.
