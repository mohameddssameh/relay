package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single worker instance. Plain, directly-instantiable class (not a Spring bean) so that
 * {@link WorkerFleet} can construct N of them, each with its own identity and schedule.
 */
public class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final int MAX_ERROR_LENGTH = 2000;

    private static final Duration CANCEL_GRACE_PERIOD = Duration.ofSeconds(5);

    static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

    // 6x ratio — long enough to tolerate GC pauses and transient hiccups without
    // false reclaims, short enough that real crashes recover in <1 minute.
    static final Duration HEARTBEAT_INTERVAL = Duration.ofSeconds(5);
    private static final Duration STALE_WORKER_THRESHOLD = Duration.ofSeconds(30);

    static final Duration RECLAIM_INTERVAL = Duration.ofSeconds(10);

    private static final String CLAIM_SQL = """
            SELECT id, type, payload, attempts
            FROM jobs
            WHERE status = 'queued' AND run_at <= now()
            ORDER BY run_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private final UUID workerId;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, JobHandler> handlersByType;
    private final BackoffStrategy backoffStrategy;
    private final ExecutorService jobExecutor;
    private final Runnable onZombieDetected;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public Worker(UUID workerId,
                  JdbcTemplate jdbcTemplate,
                  PlatformTransactionManager transactionManager,
                  ObjectMapper objectMapper,
                  Map<String, JobHandler> handlersByType,
                  BackoffStrategy backoffStrategy,
                  int poolSize,
                  Runnable onZombieDetected) {
        this.workerId = workerId;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = objectMapper;
        this.handlersByType = handlersByType;
        this.backoffStrategy = backoffStrategy;
        this.jobExecutor = Executors.newFixedThreadPool(poolSize);
        this.onZombieDetected = onZombieDetected;
    }

    public UUID getWorkerId() {
        return workerId;
    }

    public void start() {
        Timestamp now = Timestamp.from(Instant.now());
        jdbcTemplate.update(
                "INSERT INTO workers (id, last_heartbeat_at, started_at) VALUES (?, ?, ?)",
                workerId, now, now);
    }

    public void shutdown() {
        stopped.set(true);
        jobExecutor.shutdownNow();
        jdbcTemplate.update("DELETE FROM workers WHERE id = ?", workerId);
    }

    public void heartbeat() {
        if (stopped.get()) {
            return;
        }
        int rowsAffected = jdbcTemplate.update(
                "UPDATE workers SET last_heartbeat_at = ? WHERE id = ?",
                Timestamp.from(Instant.now()), workerId);
        if (rowsAffected == 0) {
            log.warn("Worker {} was reclaimed while stalled — self-shutting down", workerId);
            onZombieDetected.run();
            shutdown();
        }
    }

    public void reclaimStaleWorkers() {
        if (stopped.get()) {
            return;
        }
        transactionTemplate.executeWithoutResult(status -> {
            List<UUID> staleWorkerIds = jdbcTemplate.query(
                    "SELECT id FROM workers WHERE last_heartbeat_at < ? FOR UPDATE SKIP LOCKED",
                    (rs, rowNum) -> (UUID) rs.getObject("id"),
                    Timestamp.from(Instant.now().minus(STALE_WORKER_THRESHOLD)));

            for (UUID staleWorkerId : staleWorkerIds) {
                // Not incrementing attempts: a crashed worker isn't the job's fault, so it
                // shouldn't cost one of its limited retries.
                jdbcTemplate.update(
                        """
                        UPDATE jobs
                        SET status = 'queued', worker_id = NULL, run_at = now()
                        WHERE worker_id = ? AND status = 'running'
                        """,
                        staleWorkerId);
                jdbcTemplate.update("DELETE FROM workers WHERE id = ?", staleWorkerId);
                log.warn("Reclaimed stale worker {}", staleWorkerId);
            }
        });
    }

    public void pollAndRun() {
        if (stopped.get()) {
            return;
        }
        claimNextJob().ifPresent(this::execute);
    }

    private Optional<ClaimedJob> claimNextJob() {
        return Optional.ofNullable(transactionTemplate.execute(status -> {
            List<ClaimedJob> rows = jdbcTemplate.query(CLAIM_SQL, Worker::mapRow);
            if (rows.isEmpty()) {
                return null;
            }
            ClaimedJob row = rows.get(0);
            int attemptNumber = row.attempts() + 1;
            jdbcTemplate.update(
                    "UPDATE jobs SET status = 'running', attempts = ?, worker_id = ? WHERE id = ?",
                    attemptNumber, workerId, row.id());
            return new ClaimedJob(row.id(), row.type(), row.payloadJson(), attemptNumber);
        }));
    }

    private void execute(ClaimedJob job) {
        JobHandler handler = handlersByType.get(job.type());
        try {
            if (handler == null) {
                throw new IllegalStateException("No handler registered for job type '" + job.type() + "'");
            }
            JsonNode payload = objectMapper.readTree(job.payloadJson());
            runWithTimeout(job, handler, payload);
            jdbcTemplate.update("UPDATE jobs SET status = 'completed', worker_id = NULL WHERE id = ?", job.id());
        } catch (Exception e) {
            handleFailure(job, handler, e);
        }
    }

    private void runWithTimeout(ClaimedJob job, JobHandler handler, JsonNode payload) throws Exception {
        CountDownLatch handlerFinished = new CountDownLatch(1);
        Future<?> future = jobExecutor.submit(() -> {
            try {
                handler.handle(payload);
            } finally {
                handlerFinished.countDown();
            }
        });

        try {
            future.get(handler.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            boolean stoppedInTime = handlerFinished.await(CANCEL_GRACE_PERIOD.toMillis(), TimeUnit.MILLISECONDS);
            if (!stoppedInTime) {
                log.warn("Handler for job {} of type '{}' didn't respond to interrupt within {}s"
                                + " — worker thread may be leaked",
                        job.id(), job.type(), CANCEL_GRACE_PERIOD.toSeconds());
            }
            throw new TimeoutException(
                    "Handler for job type '" + job.type() + "' exceeded timeout of " + handler.timeout());
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private void handleFailure(ClaimedJob job, JobHandler handler, Exception e) {
        int attemptNumber = job.attempts();
        int maxAttempts = handler != null ? handler.maxAttempts() : 0;
        String errorDescription = describeError(e);
        Instant failedAt = Instant.now();

        log.error("Job {} of type '{}' failed on attempt {}/{}", job.id(), job.type(), attemptNumber, maxAttempts, e);

        transactionTemplate.executeWithoutResult(status -> {
            if (attemptNumber < maxAttempts) {
                Instant nextRunAt = failedAt.plus(backoffStrategy.nextDelay(attemptNumber));
                jdbcTemplate.update(
                        """
                        UPDATE jobs
                        SET status = 'queued', worker_id = NULL, run_at = ?, last_error = ?, last_failed_at = ?
                        WHERE id = ?
                        """,
                        Timestamp.from(nextRunAt), errorDescription, Timestamp.from(failedAt), job.id());
            } else {
                jdbcTemplate.update(
                        """
                        INSERT INTO dead_letter_jobs (id, type, payload, status, created_at, run_at, attempts, last_error, last_failed_at)
                        SELECT id, type, payload, 'failed', created_at, run_at, attempts, ?, ?
                        FROM jobs
                        WHERE id = ?
                        """,
                        errorDescription, Timestamp.from(failedAt), job.id());
                jdbcTemplate.update("DELETE FROM jobs WHERE id = ?", job.id());
            }
        });
    }

    private static String describeError(Exception e) {
        String detail = e.getClass().getName() + (e.getMessage() != null ? ": " + e.getMessage() : "");
        return detail.length() > MAX_ERROR_LENGTH ? detail.substring(0, MAX_ERROR_LENGTH) : detail;
    }

    private static ClaimedJob mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedJob(
                (UUID) rs.getObject("id"), rs.getString("type"), rs.getString("payload"), rs.getInt("attempts"));
    }

    private record ClaimedJob(UUID id, String type, String payloadJson, int attempts) {
    }
}
