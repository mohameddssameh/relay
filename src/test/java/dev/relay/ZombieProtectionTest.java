package dev.relay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "relay.worker.instances=0")
@Testcontainers
class ZombieProtectionTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Map<String, JobHandler> handlersByType;

    @Autowired
    private BackoffStrategy backoffStrategy;

    @Test
    void workerSelfShutsDownWhenItsRowWasReclaimedWhileStalled() {
        AtomicBoolean zombieCallbackInvoked = new AtomicBoolean(false);
        Worker worker = new Worker(UUID.randomUUID(), jdbcTemplate, transactionManager, objectMapper,
                handlersByType, backoffStrategy, 1, () -> zombieCallbackInvoked.set(true));
        worker.start();

        // Simulate another worker's reclaim sweep having already deleted this worker's row
        // while it was stalled (e.g. paused by a long GC) - the exact scenario heartbeat()'s
        // rowsAffected == 0 check is meant to catch.
        jdbcTemplate.update("DELETE FROM workers WHERE id = ?", worker.getWorkerId());

        worker.heartbeat();

        assertThat(zombieCallbackInvoked.get()).isTrue();
        assertThat(worker.isJobExecutorShutdown()).isTrue();

        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO jobs (id, type, payload, status, created_at, run_at, attempts)
                VALUES (?, 'zombie_test_job', '{}'::jsonb, 'queued', now(), now(), 0)
                """,
                jobId);

        worker.pollAndRun();

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM jobs WHERE id = ?", String.class, jobId);
        assertThat(status).as("a self-shut-down worker must not claim new jobs").isEqualTo("queued");
    }
}
