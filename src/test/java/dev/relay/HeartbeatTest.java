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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "relay.worker.instances=0")
@Testcontainers
class HeartbeatTest {

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
    void heartbeatUpdatesLastHeartbeatAtOverTime() {
        Worker worker = new Worker(UUID.randomUUID(), jdbcTemplate, transactionManager, objectMapper,
                handlersByType, backoffStrategy, 1, () -> { });
        worker.start();
        try {
            Timestamp first = queryHeartbeat(worker.getWorkerId());

            sleep(50);
            worker.heartbeat();
            Timestamp second = queryHeartbeat(worker.getWorkerId());

            sleep(50);
            worker.heartbeat();
            Timestamp third = queryHeartbeat(worker.getWorkerId());

            assertThat(second.toInstant()).isAfter(first.toInstant());
            assertThat(third.toInstant()).isAfter(second.toInstant());
        } finally {
            worker.shutdown();
        }
    }

    private Timestamp queryHeartbeat(UUID workerId) {
        return jdbcTemplate.queryForObject(
                "SELECT last_heartbeat_at FROM workers WHERE id = ?", Timestamp.class, workerId);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
