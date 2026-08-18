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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "relay.worker.instances=0")
@Testcontainers
class StaleWorkerReclaimTest {

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
    void staleWorkersRunningJobIsRequeuedWithoutIncrementingAttempts() {
        UUID staleWorkerId = UUID.randomUUID();
        Timestamp longAgo = Timestamp.from(Instant.now().minusSeconds(60));
        jdbcTemplate.update(
                "INSERT INTO workers (id, last_heartbeat_at, started_at) VALUES (?, ?, ?)",
                staleWorkerId, longAgo, longAgo);

        UUID jobId = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO jobs (id, type, payload, status, created_at, run_at, attempts, worker_id)
                VALUES (?, 'stale_reclaim_test_job', '{}'::jsonb, 'running', now(), now(), 2, ?)
                """,
                jobId, staleWorkerId);

        Worker reclaimer = new Worker(UUID.randomUUID(), jdbcTemplate, transactionManager, objectMapper,
                handlersByType, backoffStrategy, 1, () -> { });
        try {
            reclaimer.reclaimStaleWorkers();

            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT status, attempts, worker_id FROM jobs WHERE id = ?", jobId);
            assertThat(row.get("status")).isEqualTo("queued");
            assertThat(row.get("attempts")).isEqualTo(2);
            assertThat(row.get("worker_id")).isNull();

            Integer staleWorkerRowCount = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM workers WHERE id = ?", Integer.class, staleWorkerId);
            assertThat(staleWorkerRowCount).isZero();
        } finally {
            reclaimer.shutdown();
        }
    }
}
