package dev.relay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flagship M3 test: proves FOR UPDATE SKIP LOCKED actually prevents two concurrently-polling
 * workers from double-executing the same job.
 */
@SpringBootTest(properties = "relay.worker.instances=0")
@Testcontainers
@Import(MultiWorkerSkipLockedTest.TestHandlers.class)
class MultiWorkerSkipLockedTest {

    private static final int JOB_COUNT = 100;
    private static final Map<Integer, AtomicInteger> executionCounts = new ConcurrentHashMap<>();

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Autowired
    private RelayClient relayClient;

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
    void hundredJobs_eachRunsExactlyOnceAcrossTwoConcurrentWorkers() throws InterruptedException {
        executionCounts.clear();

        for (int i = 0; i < JOB_COUNT; i++) {
            relayClient.enqueue("count_execution", Map.of("index", i));
        }

        Worker workerA = newWorker();
        Worker workerB = newWorker();
        workerA.start();
        workerB.start();

        ExecutorService pollers = Executors.newFixedThreadPool(2);
        try {
            pollers.submit(() -> pollUntilDrained(workerA));
            pollers.submit(() -> pollUntilDrained(workerB));
            pollers.shutdown();
            boolean finishedInTime = pollers.awaitTermination(30, TimeUnit.SECONDS);
            assertThat(finishedInTime).as("both worker poll loops finished within 30s").isTrue();
        } finally {
            workerA.shutdown();
            workerB.shutdown();
        }

        assertThat(completedCount()).isEqualTo(JOB_COUNT);
        assertThat(executionCounts).hasSize(JOB_COUNT);
        executionCounts.values().forEach(count ->
                assertThat(count.get()).as("each job's handler invocation count").isEqualTo(1));
    }

    private Worker newWorker() {
        return new Worker(UUID.randomUUID(), jdbcTemplate, transactionManager, objectMapper,
                handlersByType, backoffStrategy, 4, () -> { });
    }

    private void pollUntilDrained(Worker worker) {
        Instant deadline = Instant.now().plusSeconds(25);
        while (Instant.now().isBefore(deadline) && completedCount() < JOB_COUNT) {
            worker.pollAndRun();
        }
    }

    private int completedCount() {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM jobs WHERE type = 'count_execution' AND status = 'completed'",
                Integer.class);
        return count != null ? count : 0;
    }

    @TestConfiguration
    static class TestHandlers {

        @Bean("count_execution")
        JobHandler countExecutionHandler() {
            return payload -> {
                int index = payload.get("index").asInt();
                executionCounts.computeIfAbsent(index, key -> new AtomicInteger(0)).incrementAndGet();
            };
        }
    }
}
