package dev.relay;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@SpringBootTest
@Import({TestBackoffStrategy.Config.class, RetryIntegrationTest.TestHandlers.class})
class RetryIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private RelayClient relayClient;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void failsOnceThenSucceeds_endsCompletedWithAttemptsTwo() {
        relayClient.enqueue("fails_once_then_succeeds", Map.of());

        awaitJobStatus("fails_once_then_succeeds", "completed", Duration.ofSeconds(15));

        assertThat(queryAttempts("fails_once_then_succeeds")).isEqualTo(2);
    }

    @Test
    void alwaysFails_endsUpInDeadLetterWithCorrectAttemptsAndError() {
        relayClient.enqueue("always_fails", Map.of());

        awaitDeadLettered("always_fails", Duration.ofSeconds(15));

        assertThat(jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE type = 'always_fails'", Integer.class))
                .isZero();

        Integer attempts = jdbcTemplate.queryForObject(
                "SELECT attempts FROM dead_letter_jobs WHERE type = 'always_fails'", Integer.class);
        String lastError = jdbcTemplate.queryForObject(
                "SELECT last_error FROM dead_letter_jobs WHERE type = 'always_fails'", String.class);

        assertThat(attempts).isEqualTo(2);
        assertThat(lastError).contains("RuntimeException").contains("always fails, on purpose");
    }

    @Test
    void handlerTimeout_isTreatedAsFailedAttempt() {
        relayClient.enqueue("always_times_out", Map.of());

        awaitDeadLettered("always_times_out", Duration.ofSeconds(15));

        Integer attempts = jdbcTemplate.queryForObject(
                "SELECT attempts FROM dead_letter_jobs WHERE type = 'always_times_out'", Integer.class);
        String lastError = jdbcTemplate.queryForObject(
                "SELECT last_error FROM dead_letter_jobs WHERE type = 'always_times_out'", String.class);

        assertThat(attempts).isEqualTo(1);
        assertThat(lastError).contains("TimeoutException").contains("exceeded timeout");
    }

    @Test
    void failedJob_getsRunAtPushedIntoTheFuture_notImmediateReclaim() {
        relayClient.enqueue("future_run_at_probe", Map.of());

        awaitFirstFailureRecorded("future_run_at_probe", Duration.ofSeconds(15));

        Timestamp runAt = jdbcTemplate.queryForObject(
                "SELECT run_at FROM jobs WHERE type = 'future_run_at_probe'", Timestamp.class);
        Timestamp lastFailedAt = jdbcTemplate.queryForObject(
                "SELECT last_failed_at FROM jobs WHERE type = 'future_run_at_probe'", Timestamp.class);

        assertThat(runAt).isNotNull();
        assertThat(lastFailedAt).isNotNull();
        assertThat(runAt.toInstant()).isAfter(lastFailedAt.toInstant());
    }

    private void awaitJobStatus(String type, String expectedStatus, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        String status;
        do {
            status = jdbcTemplate.queryForObject(
                    "SELECT status FROM jobs WHERE type = ? ORDER BY created_at DESC LIMIT 1",
                    String.class, type);
            if (expectedStatus.equals(status)) {
                return;
            }
            sleep();
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError("Job of type '" + type + "' never reached status '" + expectedStatus
                + "' (last seen: '" + status + "')");
    }

    private void awaitDeadLettered(String type, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        Integer count;
        do {
            count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM dead_letter_jobs WHERE type = ?", Integer.class, type);
            if (count != null && count > 0) {
                return;
            }
            sleep();
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError("Job of type '" + type + "' never showed up in dead_letter_jobs");
    }

    private void awaitFirstFailureRecorded(String type, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        do {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM jobs WHERE type = ? AND last_failed_at IS NOT NULL",
                    Integer.class, type);
            if (count != null && count > 0) {
                return;
            }
            sleep();
        } while (Instant.now().isBefore(deadline));
        throw new AssertionError("Job of type '" + type + "' never recorded a failure");
    }

    private Integer queryAttempts(String type) {
        return jdbcTemplate.queryForObject(
                "SELECT attempts FROM jobs WHERE type = ? ORDER BY created_at DESC LIMIT 1",
                Integer.class, type);
    }

    private static void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @TestConfiguration
    static class TestHandlers {

        @Bean("fails_once_then_succeeds")
        JobHandler failsOnceThenSucceedsHandler() {
            AtomicInteger callCount = new AtomicInteger(0);
            return new JobHandler() {
                @Override
                public void handle(JsonNode payload) {
                    if (callCount.incrementAndGet() == 1) {
                        throw new RuntimeException("fails on first attempt, on purpose");
                    }
                }
            };
        }

        @Bean("always_fails")
        JobHandler alwaysFailsHandler() {
            return new JobHandler() {
                @Override
                public void handle(JsonNode payload) {
                    throw new RuntimeException("always fails, on purpose");
                }

                @Override
                public int maxAttempts() {
                    return 2;
                }
            };
        }

        @Bean("always_times_out")
        JobHandler alwaysTimesOutHandler() {
            return new JobHandler() {
                @Override
                public void handle(JsonNode payload) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public int maxAttempts() {
                    return 1;
                }

                @Override
                public Duration timeout() {
                    return Duration.ofMillis(200);
                }
            };
        }

        @Bean("future_run_at_probe")
        JobHandler futureRunAtProbeHandler() {
            return new JobHandler() {
                @Override
                public void handle(JsonNode payload) {
                    throw new RuntimeException("fails, on purpose, to observe run_at");
                }
            };
        }
    }
}
