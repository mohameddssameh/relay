package dev.relay;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
class RelayIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private RelayClient relayClient;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void enqueuedJobIsPickedUpAndCompleted() throws InterruptedException {
        relayClient.enqueue("send_email", Map.of("to", "test@example.com"));

        Instant deadline = Instant.now().plusSeconds(5);
        String status;
        do {
            status = jdbcTemplate.queryForObject(
                    "SELECT status FROM jobs WHERE type = 'send_email' ORDER BY created_at DESC LIMIT 1",
                    String.class);
            if ("completed".equals(status)) {
                break;
            }
            Thread.sleep(200);
        } while (Instant.now().isBefore(deadline));

        assertThat(status).isEqualTo("completed");
    }
}
