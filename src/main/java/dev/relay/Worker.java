package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
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

@Component
public class Worker {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private static final int MAX_ERROR_LENGTH = 2000;

    private static final Duration CANCEL_GRACE_PERIOD = Duration.ofSeconds(5);

    private static final String CLAIM_SQL = """
            SELECT id, type, payload, attempts
            FROM jobs
            WHERE status = 'queued' AND run_at <= now()
            ORDER BY run_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final ObjectMapper objectMapper;
    private final Map<String, JobHandler> handlersByType;
    private final BackoffStrategy backoffStrategy;
    private final ExecutorService jobExecutor;

    public Worker(JdbcTemplate jdbcTemplate,
                  PlatformTransactionManager transactionManager,
                  ObjectMapper objectMapper,
                  Map<String, JobHandler> handlersByType,
                  BackoffStrategy backoffStrategy,
                  @Value("${relay.worker.pool-size:4}") int poolSize) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.objectMapper = objectMapper;
        this.handlersByType = handlersByType;
        this.backoffStrategy = backoffStrategy;
        this.jobExecutor = Executors.newFixedThreadPool(poolSize);
    }

    @PreDestroy
    public void shutdown() {
        jobExecutor.shutdownNow();
    }

    @Scheduled(fixedDelay = 2000)
    public void pollAndRun() {
        claimNextJob().ifPresent(this::execute);
    }

    private Optional<ClaimedJob> claimNextJob() {
        return Optional.ofNullable(transactionTemplate.execute(status -> {
            List<ClaimedJob> rows = jdbcTemplate.query(CLAIM_SQL, Worker::mapRow);
            if (rows.isEmpty()) {
                return null;
            }
            ClaimedJob job = rows.get(0);
            jdbcTemplate.update("UPDATE jobs SET status = 'running' WHERE id = ?", job.id());
            return job;
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
            jdbcTemplate.update("UPDATE jobs SET status = 'completed' WHERE id = ?", job.id());
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
        int attemptNumber = job.attempts() + 1;
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
                        SET status = 'queued', attempts = ?, run_at = ?, last_error = ?, last_failed_at = ?
                        WHERE id = ?
                        """,
                        attemptNumber, Timestamp.from(nextRunAt), errorDescription, Timestamp.from(failedAt), job.id());
            } else {
                jdbcTemplate.update(
                        """
                        INSERT INTO dead_letter_jobs (id, type, payload, status, created_at, run_at, attempts, last_error, last_failed_at)
                        SELECT id, type, payload, 'failed', created_at, run_at, ?, ?, ?
                        FROM jobs
                        WHERE id = ?
                        """,
                        attemptNumber, errorDescription, Timestamp.from(failedAt), job.id());
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
